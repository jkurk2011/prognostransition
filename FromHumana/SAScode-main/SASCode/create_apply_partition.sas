/*Purpose: Begin with a table and partition it into train/valid/test based on an identifier on
		       the file.  If the PartitionDataSet exists, it will use the partition already on there
					 and will not recalculate it.
*/
%MACRO create_apply_partition(
	idta=,
	odta=,
	PartitionDataSet= work.part /*dummy must be here if you don't have a dataset that contains some partitions*/,
	Key= /*Whatever the key identifer should be used to partition.  There can be more
			   than one row per that identifier, but all rows will be placed into the same
				 partition. */,
	TrainPct=0.6 /*What percentage of records should be placed into the training data.*/,
	ValidPct=0.4 /*What percentage of records should be placed into the validation data.
								 If TrainPct + ValidPct add up to less than 1.0, then the rest will be
								 placed into test. */);

PROC SQL;
	CREATE TABLE work.UniqRows as
	SELECT DISTINCT &Key
	FROM &idta
	;

%IF NOT %SYSFUNC(EXIST(&PartitionDataSet)) %THEN %DO;
	PROC SQL;
		CREATE TABLE work.PartitionDataSet_s0 AS
		SELECT 
		&Key, 
		'' AS partition FORMAT $6., 
		. AS cv_10fold FORMAT 8.
		FROM &idta(OBS=0)
		;
	QUIT;
%END;
%ELSE %DO; 
*This is added for back-compatibility. Older version did not produce the column cv_10fold ;
DATA work.PartitionDataSet_s0; 
	SET &PartitionDataSet; 
	cv_10fold = FLOOR(rand*10)+ 1;
RUN;
%END;

DATA work.tmp_partition(KEEP=&Key partition cv_10fold DROP=rc);
SET work.UniqRows;
  FORMAT partition $CHAR5.;

	IF _N_ = 1 THEN DO;
		DECLARE HASH hPartitionData(dataset:"work.PartitionDataSet_s0");
	 	rc = hPartitionData.definekey("&Key");
	 	rc = hPartitionData.definedata('partition','cv_10fold');
		rc = hPartitionData.definedone();
		CALL MISSING(cv_10fold);
	END;

	rc = hPartitionData.find();


RUN;

TITLE "Input partition table (if exists)";
PROC FREQ DATA=work.PartitionDataSet_s0; 
	TABLE partition/MISSING; 
RUN;
TITLE; 

DATA work.tmp_partition_nohits work.tmp_partition_hits;
	SET work.tmp_partition;
	IF partition = '' THEN OUTPUT  work.tmp_partition_nohits; 
	ELSE OUTPUT  work.tmp_partition_hits;
RUN;
PROC SQL NOPRINT; 
	SELECT DISTINCT 
		COUNT(*) INTO: no_hits_count 
	FROM work.tmp_partition_nohits; 
QUIT;

%IF %EVAL(&no_hits_count > 0) %THEN %DO;
DATA work.tmp_partition_nohits_s1; 
	SET work.tmp_partition_nohits;
	IF partition = '' THEN DO;
		rand = RAND('Uniform');
		IF rand <= &TrainPct THEN partition = 'train';
		ELSE IF rand <= %SYSEVALF(&TrainPct + &ValidPct) THEN partition = 'valid';	
		ELSE partition = 'test ';
		cv_10fold = FLOOR(rand*10)+ 1;

	END;
RUN;

DATA work.PartitionDataSet_s1; 
	SET work.tmp_partition_hits work.tmp_partition_nohits_s1 work.PartitionDataSet_s0; 
RUN;
%END;
%ELSE %DO; 
DATA work.PartitionDataSet_s1; 
	SET work.tmp_partition_hits work.PartitionDataSet_s0; 
RUN;
%END;
PROC SORT DATA=work.PartitionDataSet_s1 NODUPKEY OUT=&PartitionDataSet; 
	BY &Key; 
RUN;


DATA &odta(DROP=rc);
SET &idta;
  FORMAT partition $CHAR5.;

	IF _N_ = 1 THEN DO;
		DECLARE HASH hPartitionData(dataset:"&PartitionDataSet");
	 	rc = hPartitionData.definekey("&Key");
	 	rc = hPartitionData.definedata('partition','cv_10fold');
		rc = hPartitionData.definedone();
		CALL MISSING(cv_10fold);
	END;
	rc = hPartitionData.find();
RUN;
TITLE "Final partition frequency table ";
PROC FREQ DATA=&PartitionDataSet; 
	TABLE partition/MISSING; 
RUN;
TITLE; 

%MEND;
