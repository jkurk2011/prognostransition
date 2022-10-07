/************************************************************************************************************/
/* AUTHOR: Justin Newton
   CREATE DATE: 7/11/2017
   REVISIONS 8/20/2018--Add MAE and other related components that already existed in the fit_metrics R program.
                        Also, rename rebal to add_rebal and reslope to mult_rebal thinking that will be more
                        intuitive.
   PURPOSE:  Calculate training and validation data model fit metrics for a given predictor.  Created in a
             fairly general way.  

             In particular, rsq (r-squared), msep (mean squared error expressed as a percentage of the mean actual), and 
             maep_adj (mean absolute error, expressed as a percentage of the actual, and adjusted for bias) are all produced.  
             
             The function also produces pred_margin_improve, which is the profit margin improvement that can be expected.  This
             is only strictly applicable in the case of a new business, group-level prediction of future claims used in underwriting, 
             though it may be a useful summary in other cases as well because it combines the three default model fit metrics in an
             apples-to-apples manner.  Specifically, each of the model fit metrics is run through a quadratic regression that was
             fit to margin improvement via a simulation study.  Then the three estimates are blended together.
  idta--Name of input table
  pred--Name of prediction column in the input table
  actual--Name of actual column in the input table.
  weight--Name of column of weights to calculate weighted R-squared.  Default will just be equal weights.
  partition--Optional, name of column in the input table that identifies the partition into train, valid, and/or test.  Without this,
             it will assume the entire dataset is training.
  partition_to_rebal--Quoted string for the partition value used to calculate rebalancing, if any.  The data would be rebalanced 
                      for this partition, and then the same adjustment would be applied to the other partitions as well.
  add_rebal--Should the prediction be shifted additively so as to correctly predict the actual on average on the
             training data?  The resulting intercept will also be used on the validation data--it will not be
             refit.
  mult_rebal--Should the prediction be shifted multiplicatively?  If add_rebal=0 and mult_rebal=1 this is just uses a
               multiplicative factor to force the prediction to equal the actual on average.  add_rebal=1 and
               mult_rebal=1 is sort of like refitting the model.  This will probably only be appropriate if the
               predictor was fit on some completely different dataset or isn't even on the same scale as the
               actual.  In any case, the fitting will only be done on the train partition.
  running_tbl--Table name for the results.
  new_table--1/0 determines whether the running_tbl is created from scratch or appended to.
  marg_blend--Space-delimited three values for blending together estimates of margin improvement based on r-squared, mean squared error 
              as a percentage of the mean actual, and mean absolute error as a percentage of the mean actual.
/************************************************************************************************************/
%MACRO calc_model_fit(idta=, pred=, actual=, weight=0, partition=0, partition_to_rebal = 'train', add_rebal=0, mult_rebal=0, 
                      running_tbl=work.cmf_results, new_table=1, marg_blend=0.4 0.2 0.4);

  DATA work.calc_model_fit_s0;
  SET &idta;
    %IF &partition = 0 %THEN %DO;
      cmf_partition = 'train';
      %END;
    %ELSE %DO;
      cmf_partition = &partition;
      %END;
    %IF &weight = 0 %THEN %DO;
      cmf_weight = 1;
      %END;
    %ELSE %DO;
      cmf_weight = &weight;
      %END;
    IF UPCASE(cmf_partition) = %UPCASE(&partition_to_rebal) THEN cmf_actual_train = &actual;
    ELSE cmf_actual_train = .;
  RUN;

  %IF &add_rebal = 0 AND &mult_rebal = 0 %THEN %DO;
    DATA work.calc_model_fit_s1;
    SET work.calc_model_fit_s0;
      cmf_pred = &pred;
    RUN;   
    %END;
  %ELSE %IF &add_rebal = 0 AND &mult_rebal = 1 %THEN %DO;
    PROC SQL NOPRINT;
      SELECT DISTINCT
        SUM(&actual * cmf_weight) / SUM(&pred * cmf_weight) INTO: rebal_factor
      FROM work.calc_model_fit_s0
      WHERE UPCASE(cmf_partition) = %UPCASE(&partition_to_rebal)
      ;

     DATA work.calc_model_fit_s1;
     SET work.calc_model_fit_s0;
       cmf_pred = &pred * &rebal_factor;
     RUN;      
    %END;
  %ELSE %IF &add_rebal = 1 AND &mult_rebal = 0 %THEN %DO;
    PROC SQL NOPRINT;
      SELECT DISTINCT
        ( SUM(&actual * cmf_weight) - SUM(&pred * cmf_weight) ) / SUM(cmf_weight) INTO: rebal_factor
      FROM work.calc_model_fit_s0
      WHERE UPCASE(cmf_partition) = %UPCASE(&partition_to_rebal)
      ;

     DATA work.calc_model_fit_s1;
     SET work.calc_model_fit_s0;
       cmf_pred = &pred + &rebal_factor;
     RUN;      
    %END;
  %ELSE %IF &add_rebal = 1 AND &mult_rebal = 1 %THEN %DO;
    PROC GENMOD DATA=work.calc_model_fit_s0;
    MODEL cmf_actual_train = &pred;
    WEIGHT cmf_weight;
    OUTPUT 
      OUT=work.calc_model_fit_s1
      PRED=cmf_pred
      ;
    %END;

  PROC SQL;
    CREATE TABLE work.grand_means AS
    SELECT DISTINCT 
      cmf_partition,
      SUM(&actual * cmf_weight) / SUM(cmf_weight) AS mean_actual,
      SUM(cmf_pred * cmf_weight) / SUM(cmf_weight) AS mean_pred
    FROM work.calc_model_fit_s1
    GROUP BY cmf_partition
    ;

  PROC SQL;
    CREATE TABLE work.cmf_one_pred_results AS
    SELECT DISTINCT
      "&pred               " AS predictor,
      main.cmf_partition,
      gm.mean_actual,
      gm.mean_pred,
      COUNT(*) AS cnt,
      SUM(cmf_weight) AS total_weights,
      SUM( cmf_weight * ABS(&actual - cmf_pred) ) / SUM(cmf_weight) AS mae,
      SUM( cmf_weight * ABS(&actual - mean_actual) )/ SUM(cmf_weight) AS mae_at_actual,
      SUM( cmf_weight * ABS(&actual - mean_pred) ) / SUM(cmf_weight) AS mae_at_pred,
      SUM( cmf_weight * (&actual - cmf_pred)**2 ) AS ess,
      SUM( cmf_weight * (&actual - mean_actual)**2 ) AS tss
    FROM work.calc_model_fit_s1 main
    INNER JOIN work.grand_means gm
      ON main.cmf_partition = gm.cmf_partition
    WHERE main.cmf_partition NE ''
    GROUP BY 
      main.cmf_partition, 
      gm.mean_actual,
      gm.mean_pred
    ;  

  %LET rsq_blend = %SCAN(&marg_blend, 1, ' ');
  %LET msep_blend = %SCAN(&marg_blend, 2, ' ');
  %LET maep_blend = %SCAN(&marg_blend, 3, ' ');
  %LET blend_sum = %SYSEVALF(&rsq_blend + &msep_blend + &maep_blend);
  %PUT &=rsq_blend &=msep_blend &=maep_blend;

  DATA work.cmf_one_pred_results;
  SET work.cmf_one_pred_results;
    maep = mae / mean_actual;
    maep_adj = maep + (mae_at_actual - mae_at_pred) / mean_actual;
    IF maep_adj < maep THEN maep_adj = maep; /* The point of the adjustment is not to reward a prediction that is just too low (or high) in general. */
    mse = ess / total_weights;
    msep = SQRT(mse) / mean_actual;
    rsq = 1 - ess / tss;
    maep_pred_margin_improve = 0.33942 + 0.40321 * maep_adj - 3.61882 * maep_adj**2;
    msep_pred_margin_improve = 0.33519 + 0.39057 * msep - 2.18478 * msep**2;
    rsq_pred_margin_improve = -0.05521 + 0.60256 * rsq - 0.18725 * rsq**2;
    pred_margin_improve = (&rsq_blend * rsq_pred_margin_improve + &msep_blend * msep_pred_margin_improve + &maep_blend * maep_pred_margin_improve) / &blend_sum;
  RUN;

  %IF &new_table = 1 %THEN %DO;
    DATA &running_tbl;
    SET work.cmf_one_pred_results;
    FORMAT predictor $32.;
    RUN;
    %END;
  %ELSE %DO;
     DATA &running_tbl;
     SET &running_tbl
         work.cmf_one_pred_results;
     RUN;
     %END;

%MEND;
