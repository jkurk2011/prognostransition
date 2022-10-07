/*MACRO PROCEDURE DASPLINE

Requires:  Macro DSHIDE.

   For a given list of variables, generates formulas for dummy  variables
   that  allow fitting of Stone and Koo's additive splines constrained to
   be linear in the tails.  If the variables  are  named  A  and  B,  for
   example,  the  generated  dummy  variables  will  be  A1,A2,...,Ap and
   B1,B2,...,Bq, where p and q are the number of  knots  minus  2  (if  a
   variable name is 8 characters long, the last character is ignored when
   forming  dummy  variable names).  The spline models are then fitted by
   specifying as independent variables A A1-Ap B B1-Bq.  If  knot  points
   are  not  specified, NK knots will be selected automatically, where NK
   =3 -7.  The following quantiles are used according to NK:

        NK              Quantiles
        3       .05     .5      .95
        4       .05     .35     .65     .95
        5       .05     .275    .5      .725    .95
        6       .05     .23     .41     .59     .77     .95
        7       .025    .18333  .34166  .5      .65833  .81666  .975

   Stone and Koo (see
   3rd reference below) recommend using the following  quantiles  if  the
   sample  size  is  n:   0, 1/(1+m), .5, 1/(1+1/m), 1, where m is n**.5.
   The second percentile can be derived approximately from the following
   table:

        n     Percentile
       <23      25
     23-152     10
    153-1045     5
      1046+      1

   Instead of letting DASPLINE choose knots, knots may be given for up to
   20 variables by specifying KNOTi=knot points  separated  by  spaces,
   where  i  corresponds  to variable number i in the list.  Formulas for
   dummy variables to compute spline components for variable V are stored
   in macro _V (eighth letter of V is truncated if needed).  Knot  points
   computed  by  DASPLINE or given by the user are stored in global macro
   variables named _knot1_,_knot2_,...

   Usage:

   %DASPLINE(list of variables separated by spaces,
            NK=number of knots to use if KNOTi is not given for variable
                 i (default=4),
            KNOT1=at least 3 knot points, ... KNOT20=knot points,
            DATA=dataset to use for automatic determination of knot points
                 if not given in KNOTi (default=_LAST_) );

        norm=0 : no normalization of constructed variables
        norm=1 : divide by cube of difference in last 2 knots
                 makes all variables unitless
        norm=2 : (default) divide by square of difference in outer knots
                 makes all variables in original units of x

   References:

   Devlin TF, Weeks BJ (1986): Spline functions for logistic regression
   modeling. Proc Eleventh Annual SAS Users Group International.
   Cary NC: SAS Institute, Inc., pp. 646-51.

   Stone CJ, Koo CY (1985): Additive splines in statistics. Proc Stat
   Comp Sect Am Statist Assoc, pp. 45-8.

   Stone CJ (1986): Comment, pp. 312-314, to paper by Hastie T. and
   Tibshirani R. (1986): Generalized additive models. Statist
   Sciences 1:297-318.

   Author  : Frank E. Harrell, Jr.
             Takima West Corporation
             Clinical Biostatistics, Duke University Medical Center
   Date    : 15 July 86
   Modified: 27 Aug  86 - added GLOBAL definitions for _knoti_
             28 Aug  86 - added NORM option
             23 Sep  86 - added %QUOTE to %IF &&knot etc.,OUTER
             24 Sep  86 - tolerance check on knots close to zero
             04 Oct  86 - added SECOND and changed NK default to 5
             19 May  87 - changed NK default to 4
             08 Apr  88 - modified for SAS Version 6
             14 Sep  88 - Added DSHIDE
             03 Jan  90 - modified for UNIX
             10 Apr  90 - modified to use more flexible knots with
                          UNIVARIATE 6.03
             06 May  91 - added more norm options, changed default
             10 May  91 - fixed bug re precedence of <>

                                                                      */
%MACRO daspline(x,nk=4,knot1=,knot2=,knot3=,knot4=,knot5=,knot6=,knot7=,
       knot8=,knot9=,knot10=,knot11=,knot12=,knot13=,knot14=,knot15=,
       knot16=,knot17=,knot18=,knot19=,knot20=,
       norm=2,data=_LAST_);
%LOCAL i j needknot nx lastds v v7 k tk tk1 t t1 k2 low hi slow shi kd;
%LOCAL _e_1 _e_2 _e_3 _e_4 _e_5 _e_6 _e_7 _e_8 _e_9;
%LOCAL _lastds_;
%DSHIDE;
%LET x=%SCAN(&x,1,'"''');
%LET needknot=0; %LET nx=0;
%*Strip off quotes in KNOTs and see if any KNOTS unspecified;
     %DO i=1 %TO 20;
     %IF %SCAN(&x,&i)=  %THEN %GOTO nomorex;
     %LET nx=%EVAL(&nx+1);
     %IF %QUOTE(&&knot&i)=  %THEN %LET needknot=%EVAL(&needknot+1);
     %ELSE %LET knot&i=%SCAN(&&knot&i,1,'"''');
     %END;
%nomorex:
%IF &needknot^=0 %THEN %DO;
  %LET lastds=&sysdsn; %IF &lastds^=_NULL_ %THEN
    %LET lastds=%SCAN(&sysdsn,1).%SCAN(&sysdsn,2);
  RUN;OPTIONS NONOTES;
  PROC UNIVARIATE DATA=&data NOPRINT;VAR
    %DO i=1 %TO &nx;
    %IF %QUOTE(&&knot&i)=  %THEN %SCAN(&x,&i);
    %END;
  ; OUTPUT OUT=_stats_ pctlpts=
        %IF &nk=3 %THEN 5 50 95;
        %IF &nk=4 %THEN 5 35 65 95;
        %IF &nk=5 %THEN 5 27.5 50 72.5 95;
        %IF &nk=6 %THEN 5 23 41 59 77 95;
        %IF &nk=7 %THEN 2.5 18.3333 34.1667 50 65.8333 81.6667 97.5;
   PCTLPRE=x1-x&needknot PCTLNAME=p1-p&nk;
  DATA _NULL_;SET _stats_;
  %*For knot points close to zero, set to zero;
  ARRAY _kp_ _NUMERIC_;DO OVER _kp_;IF ABS(_kp_)<1E-9 THEN _kp_=0;END;
    %LET j=0;
    %DO i=1 %TO &nx;
    %IF %QUOTE(&&knot&i)=  %THEN %DO;
      %LET j=%EVAL(&j+1);
      CALL SYMPUT("knot&i",
      TRIM(LEFT(x&j.p1))
        %DO k=2 %TO &nk;
        ||" "||TRIM(LEFT(x&j.p&k))
        %END;
      );
      %END;
    %END;
  RUN; OPTIONS NOTES _LAST_=&lastds;
  %END;
  %DO i=1 %TO &nx;
  %GLOBAL _knot&i._; %LET _knot&i._=&&knot&i;
  %PUT Knots for %SCAN(&x,&i):&&knot&i;
  %END;
/*Generate code for calculating dummy variables*/;

	DATA &data;
	SET &data;
  %DO i=1 %TO &nx;
	  %PUT Running Loop;
  %LET v=%SCAN(&x,&i); %IF %LENGTH(&v)=32 %THEN %LET v7=%SUBSTR(&v,1,31);
  %ELSE %LET v7=&v;
  %GLOBAL _&v7; %LET _&v7=;
  /*Get no. knots, last knot, next to last knot;*/
    %DO k=1 %TO 99;
    %IF %QUOTE(%SCAN(&&knot&i,&k,%STR( )))=  %THEN %GOTO nomorek;
    %END;
  %nomorek: %LET k=%EVAL(&k-1); %LET k2=%EVAL(&k-2);
  %LET tk=%SCAN(&&knot&i,&k,%STR( ));
  %LET tk1=%SCAN(&&knot&i,%EVAL(&k-1),%STR( ));
  %LET t1=%SCAN(&&knot&i,1,%STR( ));
  %IF &norm=0 %THEN %LET kd=1;
  %ELSE %IF &norm=1 %THEN %LET kd=(&tk - &tk1);
  %ELSE %LET kd=((&tk - &t1)**.666666666666);

  %DO j=1 %TO &k2;
    %LET t=%SCAN(&&knot&i,&j,%STR( ));
			&v7&j=max((&v-&t)/&kd,0)**3+((&tk1-&t)*max((&v-&tk)/&kd,0)**3
			        -(&tk-&t)*max((&v-&tk1)/&kd,0)**3)/(&tk-&tk1);
    %END;
  %END;
	RUN;
OPTIONS _LAST_ = &_lastds_;
%MEND;
  /*  SAS MACRO DSHIDE

Macro procedure used to ensure that the default data set
remains the same after a macro procedure call as it was
prior to the call.  DSHIDE is to be used within another
macro therefore the %LOCAL and OPTIONS statements illustrated
below are very improtant to the use of DSHIDE.

USAGE: Assume DSHIDE is used in a macro called x:

%MACRO x(...parameters......);
            .
            .
  (Statements prior to first data step in Macro X)
            .
            .
%LOCAL _lastds_;
%DSHIDE;
            .
            .
  (Statements until end of Macro X)
            .
            .
OPTIONS _LAST_ = &_lastds_;
%MEND x;

Author: Steve Peck and Frank Harrell
Date  : 3 Aug 88
                                      */

%MACRO dshide;
%LET _lastds_ = &sysdsn;
%IF &_lastds_ ^= _NULL_ %THEN
%IF %SCAN(&sysdsn,2) ^= %THEN %DO;
     %LET _lastds_ = %SCAN(&sysdsn,1).%SCAN(&sysdsn,2); %END;
%ELSE %DO; %LET _lastds_ = %SCAN(&sysdsn,1); %END;
%MEND dshide;
