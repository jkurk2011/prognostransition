/*This macro can take data in which there are multiple rows for a given "key", each with 
a value for "multinom_var" and create one row for each "key" with 
column names equal to concatenate("prefix","multinom_var") and label
for that column equal to "lab".
The columns contain the number of times that each "multinom_var" appeared
fro a given "key".  This number may be 0, 1, or greater than 1.

If you want code that returns only 0 or 1, you need code like this:
DATA databrkr.nb_5199_debit_model;
   SET work.formodel3;
   array change prefix_: ;
        DO over change;
			IF change in(.,0) THEN change=0;
			else if change>0 then change=1;
	        ELSE change=change;
        END;
*/
%MACRO multinomial_to_binary(indata = , 
															outdata = ,
															multinom_var = ,
															keys = ,
															prefix = ,
															lab=);

PROC SORT DATA=&indata(keep=&multinom_var &lab) out=work._unique_&multinom_var nodupkey;
	BY &multinom_var;
RUN;



DATA _NULL_; 
  SET work._unique_&multinom_var END=end; 
  IF _N_ EQ 1 THEN DO; 
    CALL EXECUTE("DATA work._create_flags_s1;"); 
    CALL EXECUTE("SET &indata;"); 
    END; 
		CALL EXECUTE(CAT("IF COMPRESS(&multinom_var) = '", COMPRESS(&multinom_var),"' THEN &prefix.", COMPRESS(&multinom_var)," = 1 ","; ELSE &prefix.", COMPRESS(&multinom_var)," = 0;"));
		%IF  &LAB= %THEN %DO; 	%END;	
		%ELSE %DO ; CALL EXECUTE(CAT("label &prefix.", COMPRESS(&multinom_var)," = '",COMPRESS(&lab),"';"));
		%END;

IF END THEN CALL EXECUTE('RUN;'); 
RUN; 


/*PROC MEANS DATA=work._create_flags_s1( */
/*  DROP=&multinom_var */
/*  ) NWAY; */
/*  	CLASS &keys; */
/*	VAR &prefix.: ; */
/*  OUTPUT */
/*    OUT=&outdata(DROP = _TYPE_ RENAME=(_FREQ_ = &prefix.cnts) ) */
/*    SUM= */
/*    /;*/
/*QUIT;*/

PROC means DATA=work._create_flags_s1( 
  DROP= &multinom_var 
  ) NWAY noprint; 
  	CLASS &keys; 
 	VAR &prefix.: ;  
  OUTPUT 
    OUT=&outdata(DROP = _TYPE_ )  
    max= 
    /;
QUIT;

%MEND;
