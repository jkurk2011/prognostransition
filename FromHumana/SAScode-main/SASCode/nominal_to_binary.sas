/*save a change */


/* A macro for dumming coding */
%MACRO nominal_to_binary(
  sm_dataset=/* data set */, 
  sm_var= /* categorical variable */, 
  sm_prefix= /* prefix for dummy variables */);

/* Find the unique levels of the categorical variable */
proc sort data=&sm_dataset(keep=&sm_var) out=&sm_dataset._unique nodupkey;
by &sm_var;
run;
data _null_;
set &sm_dataset._unique end=end;
/* Use CALL EXECUTE to dynamically create a macro that executes */
/* after this DATA step finishes. The metaprogrammed macro */
/* modifies the original data set. */
if _N_ eq 1 then do;
call execute("data &sm_dataset;");
call execute("set &sm_dataset;");
end;
call execute(cat("length &sm_prefix", &sm_var," 3;")); /* use minimum storage */
call execute(cats("&sm_prefix", &sm_var," = &sm_var = '", &sm_var,"';"));
if end then call execute('run;');
run;
proc sql;
/* Clean up */
drop table &sm_dataset._unique;
quit;

%MEND;

/* Example invocation */

/*Replaces original dataset with dataset containing dummy coded variables the names of which
end with the values of the original class variable*/
/*%nominal_to_binary(sm_dataset=rmtwork.off_2014model, sm_var=state, sm_prefix=state_);*/

/*adds a second set of dummy coded variables to the dataset*/
/*%nominal_to_binary(sm_dataset=rmtwork.off_2014model, sm_var=sic_cat, sm_prefix=sic_num);*/
