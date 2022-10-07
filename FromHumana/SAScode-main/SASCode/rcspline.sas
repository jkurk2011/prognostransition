/*
	This is a MACRO written by Prof. Frank Harrell to do "restricted
	cubic splines" in SAS.  It can be used within a data step to define splines.
	If you specify k knots than the macro will generate k-2 new variables by
	adding numbers to the end of the "x" variable.  Prof Harrell has rules for
	selecting the knots, for example for 3 knots one should select the 10%, 50%
	and 90% of the x-distribution.  For 4 knots use5%, 35%, 65%, and 95%.  Often
	we use the percentiles of cases when doing logistic regression.
*/
%MACRO RCSPLINE(x,knot1,knot2,knot3,knot4,knot5,knot6,knot7,knot8,knot9,knot10,norm=2);
  %LOCAL j v7 k tk tk1 t k1 k2;
  %LET v7=&x; %IF %LENGTH(&v7)=8 %THEN %LET v7=%SUBSTR(&v7,1,7);
      %*Get no. knots, last knot, next to last knot;
        %DO k=1 %TO 10;
        %IF %QUOTE(&&knot&k)=  %THEN %GOTO nomorek;
        %END;
  %LET k=11;
  %nomorek: %LET k=%EVAL(&k-1); %LET k1=%EVAL(&k-1); %LET k2=%EVAL(&k-2);
  %IF &k<3 %THEN %PUT ERROR: <3 KNOTS GIVEN.  NO SPLINE VARIABLES CREATED.;
          %ELSE %DO;
          %LET tk=&&knot&k;
          %LET tk1=&&knot&k1;
          DROP _kd_; _kd_=
          %IF &norm=0 %THEN 1;
          %ELSE %IF &norm=1 %THEN &tk - &tk1;
          %ELSE (&tk - &knot1)**.666666666666; ;
                  %DO j=1 %TO &k2;
                   %LET t=&&knot&j;

  &v7&j=max((&x-&t)/_kd_,0)**3+((&tk1-&t)*max((&x-&tk)/_kd_,0)**3
                     -(&tk-&t)*max((&x-&tk1)/_kd_,0)**3)/(&tk-&tk1)%STR(;);
                  %END;
     %END;
  %MEND;
