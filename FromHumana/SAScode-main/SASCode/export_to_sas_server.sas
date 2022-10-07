
%MACRO export_to_sas_server(export_data = ,
destination_name = ,
destination_dir = , 
file_extension = csv, 
delim = '|');

PROC CONTENTS DATA=&export_data OUT=work.names NOPRINT;
RUN;

PROC SQL NOPRINT;
SELECT '"'||trim(name)||'"',
         CASE
           WHEN type=2 THEN 'put '||trim(name)||' $ '
           ELSE 'put '||trim(name)
               END
    INTO :vars
      separated by "&delim",
        :data
          separated by ' @ ;'
	FROM work.names
	ORDER BY varnum;
QUIT;

DATA _NULL_;
  FILE "&destination_dir./&destination_name..&file_extension." delimiter=&delim DSD DROPOVER lrecl=32767 termstr=crlf; *Windows;
  IF _N_ = 1 THEN DO;
   PUT &vars.;
  END;
  SET  &export_data.;
  &data.;
RUN;

%MEND;
