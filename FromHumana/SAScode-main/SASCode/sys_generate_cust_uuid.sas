* encrypt src_rpt_cust_id to generate a group_id;
 /*the original encrypted group id list is at ubpk3381.prognos_src_cust_id_lookup datasga.prognos_src_cust_id_lookup, 
	and R:\EmployerGroup\Analytics\New Data Sources (Vetting and Contracting)\Prognos\POCDataExchange\prognos_group_id_lookup.xlsx
*/
/*A UUID (Universal Unique Identifier) is a 128-bit number used to uniquely */
/*identify some object or entity on the Internet. ... Because the network */
/*address identifies a unique computer, and the timestamp is unique for*/
/*each UUID generated from a particular host,*/
/*those two components should sufficiently ensure uniqueness.*/
%MACRO sys_generate_cust_uuid(idta=,
	odta=,
	encryptedgroupidset= work.existing_lookup /*dummy must be here if you don't have a dataset that contains some partitions*/
);
/*at some point macro can be adjusted to all encryption of pers gen key as well*/
PROC SQL;
	CREATE TABLE work.UniqRows_s0 as
	SELECT DISTINCT src_rpt_cust_id
	FROM &idta
	;

%IF NOT %SYSFUNC(EXIST(&encryptedgroupidset)) %THEN %DO;
	PROC SQL;
		CREATE TABLE &encryptedgroupidset AS
		SELECT 
			src_rpt_cust_id,
			'' as src_rpt_cust_uuid format = $30.
		FROM &idta(OBS=0)
		;
	QUIT;
%END;

PROC SQL;
	CREATE TABLE work.UniqRows as
	SELECT * from work.uniqrows_s0
	where src_rpt_cust_id not in
		(select distinct 
		src_rpt_cust_id 
		from &encryptedgroupidset
		)
	;
DATA work.distinct_src_cust_id_s1; 
	SET work.uniqrows;
	LENGTH group_id1-group_id5 $30.;
	LENGTH src_rpt_cust_uuid $30.;
	 group_id1 = uuidgen(1,0);

RUN;
DATA work.distinct_src_cust_id_s1; 
	SET work.distinct_src_cust_id_s1;
	 group_id2 = uuidgen(1,0);
RUN;
DATA work.distinct_src_cust_id_s1; 
	SET work.distinct_src_cust_id_s1;
	 group_id3 = uuidgen(1,0);
RUN;
DATA work.distinct_src_cust_id_s1; 
	SET work.distinct_src_cust_id_s1;
	 group_id4 = uuidgen(1,0);
RUN;

DATA work.distinct_src_cust_id_s1; 
	SET work.distinct_src_cust_id_s1;
	 group_id5 = uuidgen(1,0);
RUN;

DATA work.distinct_src_cust_id_s1; 
	SET work.distinct_src_cust_id_s1;
	rand1to5 = 1 + ROUND(4*Ranuni(327),1);
	IF rand1to5 = 1 THEN src_rpt_cust_uuid = group_id1; 
	ELSE IF rand1to5 = 2 THEN src_rpt_cust_uuid = group_id2; 
	ELSE IF rand1to5 = 3 THEN src_rpt_cust_uuid = group_id3; 
	ELSE IF rand1to5 = 4 THEN src_rpt_cust_uuid = group_id4; 
	ELSE IF rand1to5 = 5 THEN src_rpt_cust_uuid = group_id5; 
RUN;

DATA &encryptedgroupidset; 
	SET &encryptedgroupidset work.distinct_src_cust_id_s1(KEEP=src_rpt_cust_id src_rpt_cust_uuid) ; 
RUN;
proc sql; create table &odta as 
select 
	b.src_rpt_cust_uuid, 
	a.*
from &idta a
left join &encryptedgroupidset b
on a.src_rpt_cust_id = b.src_rpt_cust_id
;
%MEND;
