create or replace function pivotfunction_fed() RETURNS void AS $BODY$
DECLARE concept text;
DECLARE valtype text;
DECLARE type_name text;
DECLARE query text;
DECLARE cdesQuery text;
DECLARE table_name text;
DECLARE char_val text;
DECLARE alreadyinserted text;
DECLARE num_val numeric(18,5);
DECLARE usedid text;
DECLARE currentid text;
DECLARE sex text;
DECLARE year integer;
DECLARE countCDEcolumns integer;
DECLARE startdate timestamp without time zone;
DECLARE subjectcode text;
DECLARE subjectcodeide text;
DECLARE subjectageyears integer;
DECLARE subjectage numeric(18,5);
DECLARE thisdate timestamp;
DECLARE mridate timestamp;
DECLARE this_num character varying(100);
DECLARE thisage numeric(18,5);
DECLARE p_num character varying(100);
DECLARE encounternummri character varying(100);
DECLARE gender text;
DECLARE dataset text;
DECLARE dataset_const text;
DECLARE agegroup text;-- in this pivotfunction version agegroup s value is still calculated here (considering the harmonized i2b2 does NOT have it).. in the next version (NEW2) we will consider that the harmonized i2b2 already has calculated it
DECLARE encounter text;
DECLARE countUpdates integer;
DECLARE countInserts integer;
DECLARE query2write text;
BEGIN
query := '';
usedid := '';
countUpdates := 0;
countInserts := 0;
cdesQuery := ''; countCDEcolumns :=0;

for concept in
	SELECT concept_cd FROM concept_dimension WHERE sourcesystem_cd='CDE' ORDER BY upload_id --in the mapping-task we have to have filled in upload_id with the sequence of the cde columns !!!!
loop
	cdesQuery := cdesQuery || ', "' || concept || '" text';
	countCDEcolumns := countCDEcolumns+1;
	raise notice 'CDE: %', concept;
end loop;

-- create table for cde variables - the list of variables in this table is fixed (cde variables)
table_name := 'new_table';
query := 'CREATE TABLE ' || table_name || '( "subjectcode" text' || cdesQuery;
raise notice 'Counted % CDE-columns', countCDEcolumns;

-- collect all hospital specific concept_cds in order to obtain the available columns
raise notice '1. Here is the CREATE TABLE query:: %', query;
raise notice '\n';
for concept, valtype in 
	select distinct on (concept_cd) concept_cd, valtype_cd 
	from observation_fact
loop
  CASE
    WHEN 'N'=valtype THEN
	type_name := ' numeric(18,5)';
    WHEN 'T'=valtype THEN
	type_name := ' text';
    ELSE 
	type_name := NULL; -- *****
  END CASE;
  if query = '' then
	query := query || '"' || concept || '"' || type_name ;
  else
	-- if the concept_cd has not been inserted before
	if query not like ('%"' || concept || '"%') then
	   -- add it in the create statement
	   query := query || ',' || '"' || concept || '"' || type_name ;
	   raise notice '--- added no-CDE variable: % ---', concept;
	end if;
  end if;
end loop;
--raise notice '2. Here is the CREATE TABLE query: %', query;
-- add primary key
query := query || ', PRIMARY KEY (subjectcode))';
--raise notice '3. And here it is now: %', query;
-- execute statement
execute format('DROP TABLE IF EXISTS ' || table_name);
--raise notice '4. And here it is now: %', query;
execute format(query);
-- Table has been created. Time to populate it. 
EXECUTE FORMAT('
DROP VIEW IF EXISTS valid_diagnosis_encounter_nums;
CREATE VIEW valid_diagnosis_encounter_nums AS
	SELECT encounter_num
	FROM observation_fact
	WHERE concept_cd=''DIAG_etiology_1'' AND tval_char IS NOT NULL AND tval_char NOT LIKE ''Diagnostic en%%''');

--Need to store information other than concept_cd, like subjectageyears, subjectage, gender, dataset, and agegroup  <--- CHANGE: subjectcode FROM NOW ON IS patient_ide NOT patient_num <---- !!!!
EXECUTE FORMAT('DROP TABLE IF EXISTS temp_table');
EXECUTE FORMAT('CREATE TABLE temp_table (patient_num character varying(100), mri_date timestamp, encounter_num_mri character varying(100), diag_date timestamp, encounter_num_diag character varying(100), age_diag numeric(18,5), encounter_num_mmse character varying(100), encounter_num_moca character varying(100), CONSTRAINT pkey PRIMARY KEY (patient_num))');
EXECUTE FORMAT('INSERT INTO temp_table SELECT patient_num, min(start_date) FROM visit_dimension WHERE location_cd=''MRI'' GROUP BY patient_num');
EXECUTE FORMAT('UPDATE temp_table AS tt SET encounter_num_mri=vd.encounter_num FROM visit_dimension AS vd WHERE tt.patient_num=vd.patient_num AND tt.mri_date=vd.start_date AND vd.location_cd=''MRI''');--if there is MORE than one encounter with MRI in the same date then we have a problem I guess...
FOR subjectcode, mridate, encounternummri IN SELECT patient_num, mri_date, encounter_num_mri FROM temp_table
LOOP
  FOR p_num, this_num, thisdate, thisage IN
				   SELECT patient_num, encounter_num, start_date, patient_age FROM visit_dimension
				   WHERE patient_num=subjectcode AND (SELECT ABS(EXTRACT(epoch FROM (start_date-mridate))/(3600*4)))<=180  -- time interval less or equal than 180 days (6 months)
			           AND encounter_num!=encounternummri 
				   ORDER BY (SELECT ABS(EXTRACT(epoch FROM (start_date-mridate)))) DESC
  LOOP
     IF (this_num IN (SELECT * FROM valid_diagnosis_encounter_nums)) THEN
         EXECUTE FORMAT('UPDATE temp_table SET encounter_num_diag = '''||this_num||''', diag_date = '''||thisdate||''', age_diag = '||thisage||' WHERE encounter_num_mri = ''' || encounternummri||'''');
	 --raise notice 'found valid diagnosis for encounter_mri: %  ', encounternummri;
	 EXECUTE FORMAT('UPDATE temp_table AS tt SET encounter_num_mmse = encounter_num FROM observation_fact AS obf
			 WHERE tt.patient_num=obf.patient_num AND obf.concept_cd=''minimentalstate''
			 AND (SELECT ABS(EXTRACT(epoch FROM (obf.start_date-tt.diag_date))/(3600*4)))<=180');
	 EXECUTE FORMAT('UPDATE temp_table AS tt SET encounter_num_moca = encounter_num FROM observation_fact AS obf
			 WHERE tt.patient_num=obf.patient_num AND obf.concept_cd=''montrealcognitiveassessment''
			 AND (SELECT ABS(EXTRACT(epoch FROM (obf.start_date-tt.diag_date))/(3600*4)))<=180');
	
	 EXIT;	--found it, exit the inner loop..
     END IF;
  END LOOP;
END LOOP;
EXECUTE FORMAT('DELETE FROM temp_table WHERE encounter_num_diag IS NULL');--DELETE 189 patients not having a VALID diagnosis

EXECUTE 'SELECT DISTINCT(provider_id) FROM observation_fact' INTO dataset_const;--supposedly there is only one value for provider...

raise notice 'dataset variable has value: %', dataset_const;

FOR subjectcodeide, subjectageyears, subjectage, gender, dataset in
     SELECT pm.patient_ide, FLOOR(tt.age_diag), ROUND((tt.age_diag-FLOOR(tt.age_diag))*12), pd.sex_cd, 'CLM'
	FROM temp_table tt, patient_mapping pm, patient_dimension pd
        WHERE tt.patient_num=pm.patient_num AND pm.patient_num=pd.patient_num
	--ORDER BY pd.patient_num
LOOP
currentid := ',' || subjectcodeide || ',';

   CASE
	WHEN subjectageyears is null
		THEN agegroup = null;
	WHEN 50 <= subjectageyears and subjectageyears < 60
		THEN agegroup = '''50-59y''';
	WHEN 60 <= subjectageyears and subjectageyears < 70
		THEN agegroup = '''60-69y''';
	WHEN 70 <= subjectageyears and subjectageyears < 90
		THEN agegroup = '''70-79y''';
	WHEN 80 <= subjectageyears
		THEN agegroup = '''+80y''';
	ELSE
		agegroup = '''-50y''';
   END CASE;

	IF ( usedid ~ currentid) then
	else
		
		IF subjectageyears IS NULL AND subjectage IS NULL THEN
			execute format('insert into ' || table_name  || '( subjectcode, gender, dataset, agegroup) VALUES (''' || subjectcodeide ||  ''',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		ELSIF subjectageyears IS NULL AND subjectage IS NOT NULL THEN
			execute format('insert into ' || table_name  || '( subjectcode, subjectage, gender, dataset, agegroup) VALUES (''' || subjectcodeide || ''','  || subjectage || ',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		ELSIF subjectageyears IS NOT NULL AND subjectage IS NULL THEN
			execute format('insert into ' || table_name  || '( subjectcode, subjectageyears, gender, dataset, agegroup) VALUES (''' || subjectcodeide || ''',' || subjectageyears || ',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		ELSE
		execute format('insert into ' || table_name  || '( subjectcode, subjectageyears, subjectage, gender, dataset, agegroup) VALUES (''' || subjectcodeide || ''',' || subjectageyears || ',' || subjectage || ',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		usedid := usedid || ',' || subjectcodeide ||',';
		END IF;
		countInserts=countInserts+1;
	end if;
END LOOP;
-- Demographics info has been stored
raise notice 'countInserts: %', countInserts;
-- Store data. For every concept_cd and every patient store the data that are the OLDEST <---- Selecting for each observation every time!!... but there is no other way... 
-- Now we did it with the 6-months window criteria.... Now we need to take data from all 4 selected encounters: encounter_num_mri, encounter_num_diag, encounter_num_mmse & encounter_num_moca!!!
-- ... meaning we take all observations from these 4 encounters for every patient... BUT have to be careful NOT to write over values... is it possible?.. 
-- -YES: supposing there is no intersection in these encounters.... we have to be carefull cause some of them are the same!! i.e. moca and mmse done in the same encounter ;-) ...
for concept in SELECT column_name FROM information_schema.columns WHERE information_schema.columns.table_name = 'new_table'
loop
	FOR subjectcodeide, encounter, valtype, char_val, num_val IN
	  (SELECT pm.patient_ide, obf.encounter_num, obf.valtype_cd, obf.tval_char, obf.nval_num 
	   FROM observation_fact obf, temp_table tt, patient_mapping pm
	   WHERE (obf.encounter_num=tt.encounter_num_mri OR obf.encounter_num=tt.encounter_num_diag OR obf.encounter_num=tt.encounter_num_mmse OR obf.encounter_num=tt.encounter_num_moca)
	   	  AND obf.concept_cd=concept AND obf.patient_num=pm.patient_num)
	LOOP
		--raise notice 'valtype - char_val - num_val -patient_ide(subjectcodeide)-: % - % - % -%-', valtype, char_val, num_val, subjectcodeide;
		--raise notice 'concept: %', concept;
		EXECUTE FORMAT('SELECT "'|| concept ||'" FROM '||table_name||' WHERE subjectcode = '' || subjectcodeide || ''') INTO alreadyinserted;
		IF alreadyinserted!='' AND alreadyinserted IS NOT NULL THEN CONTINUE; END IF;
		--raise notice 'Going to write a clinical variable........ alreadyinserted was %', alreadyinserted;
		CASE
		    WHEN 'T'=valtype -- the problem is when valtype is NOT T and num_val IS NULL.....
		    THEN
			IF char_val IS NULL THEN
				char_val = '';
			END IF;
			
			execute format('update ' || table_name  || ' set ' || '"' || concept || '" = ''' ||  char_val || ''' where subjectcode = ''' || subjectcodeide || '''' );--only one output row per patient
			countUpdates=countUpdates+1;
			--raise notice '--- Updated table % variable % = % for PAtient %', table_name, concept, char_val, subjectcodeide;
		    WHEN 'N'=valtype
		    THEN	
			IF num_val IS NOT NULL THEN
				execute format('UPDATE ' || table_name  || ' set ' || '"' || concept || '" = ' ||  num_val || ' where subjectcode = ''' || subjectcodeide || '''' );
			countUpdates=countUpdates+1;
			--raise notice '--- Updated table % variable % = % for PAtient %', table_name, concept, num_val, subjectcodeide;
			END IF;		
			
		  --  ELSE			
			--execute format('UPDATE ' || table_name  || ' set ' || '"' || concept || '" = ' ||  NULL || ' where subjectcode = ' || subjectcodeide );			countUpdates=countUpdates+1;
		END CASE;
	end loop;
end loop;

raise notice 'CountUpdates: %', countUpdates;
--countUpdates := execute format('SELECT COUNT(*) FROM ' || table_name);
--raise notice 'new_table has % tuples', countUpdates;

BEGIN
	COPY new_table FROM '/tmp/harmonized_clinical_data_anon.csv' DELIMITER ',' CSV HEADER ; 
EXCEPTION
WHEN OTHERS 
        THEN raise notice 'No such file /tmp/harmonized_clinical_data_anon.csv';
END;
COPY (SELECT * FROM new_table) TO '/tmp/harmonized_clinical_data_anon.csv' WITH CSV DELIMITER ',' HEADER;
EXECUTE FORMAT('DROP TABLE IF EXISTS temp_table');
EXECUTE FORMAT('DROP TABLE IF EXISTS ' || table_name);
END; $BODY$ language plpgsql;
select pivotfunction_fed();
