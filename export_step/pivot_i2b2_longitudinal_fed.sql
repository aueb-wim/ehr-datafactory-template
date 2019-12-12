create or replace function pivotfunction_long_fed() RETURNS void AS $BODY$
-- ******** OFFICIAL longitudinal export plpg function 4 i2b2-harmonized db ********
DECLARE concept text;
DECLARE valtype text;
DECLARE type_name text;
DECLARE query text;
DECLARE cdesQuery text;
DECLARE table_name text;
DECLARE char_val text;
DECLARE alreadyinserted text;
DECLARE num_val numeric(18,5);
--DECLARE usedid text;
--DECLARE currentid text;
DECLARE countCDEcolumns integer;
--DECLARE startdate timestamp without time zone;
DECLARE subjectageyears integer;
DECLARE subjectage integer;--[0,11]
DECLARE subjectcodeide text;
DECLARE subjectvisitdate timestamp;
DECLARE subjectvisitide text;
DECLARE gender text;
DECLARE dataset_const text;
DECLARE agegroup text;-- in this pivotfunction version agegroup s value is still calculated here (considering the harmonized i2b2 does NOT have it).. in the next version (NEW2) we will consider that the harmonized i2b2 already has calculated it ---> 01-12-2019: Changed my mind on that...
DECLARE encounter text;
DECLARE encounter_ide text;
DECLARE countUpdates integer;
DECLARE countInserts integer;
DECLARE countLoops integer;
BEGIN
query := '';
--usedid := '';
countUpdates := 0;
countInserts := 0;
countLoops := 0;
cdesQuery := ''; countCDEcolumns :=0;

for concept in
	SELECT concept_cd FROM concept_dimension WHERE sourcesystem_cd='CDE' ORDER BY upload_id --in the mapping-task we have to have filled in upload_id with the sequence of the cde columns !!!!
loop
	cdesQuery := cdesQuery || ', "' || concept || '" text';
	countCDEcolumns := countCDEcolumns+1;
	raise notice 'CDE: %', concept;
end loop;
raise notice 'Counted % CDE-columns', countCDEcolumns;
-- create table for cde variables - the list of variables in this table is fixed (cde variables)
table_name := 'new_table';
query := 'CREATE TABLE ' || table_name || '( "subjectcode" text, "subjectvisitid" text, "subjectvisitdate" timestamp' || cdesQuery;

-- collect all hospital specific concept_cds in order to obtain the available columns
--raise notice '1... Here is the CREATE TABLE query: %', query;
raise notice '';
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
query := query || ', PRIMARY KEY (subjectcode, subjectvisitid) )';-- in the same date a patient may have more than one visits...
--raise notice '3. And here it is now: %', query;
-- execute statement
execute format('DROP TABLE IF EXISTS ' || table_name);
--raise notice '4. And here it is now just before it runs..: %', query;
execute format(query);

EXECUTE 'SELECT DISTINCT(provider_id) FROM observation_fact LIMIT 1' INTO dataset_const;--supposedly provider has only one value... We ll give it to 'dataset' column.
raise notice 'dataset variable has value: %', dataset_const;
--Table has been created. Time to insert tuples...  <--- CHANGE: subjectcode FROM NOW ON IS patient_ide NOT patient_num <---- !!!!
FOR subjectcodeide, subjectvisitide, subjectvisitdate, subjectageyears, subjectage, gender IN

	SELECT em.patient_ide, em.encounter_ide, vd.start_date, FLOOR(vd.patient_age), ROUND((vd.patient_age-FLOOR(vd.patient_age))*12), pd.sex_cd
	FROM encounter_mapping em, visit_dimension vd, patient_dimension pd
        WHERE em.encounter_num=vd.encounter_num AND vd.patient_num=pd.patient_num
	--ORDER BY pd.patient_num
LOOP
   countLoops:=countLoops+1;
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
		
	IF subjectageyears IS NULL AND subjectage IS NULL THEN
		execute format('insert into ' || table_name  || '( subjectcode, subjectvisitid, subjectvisitdate, gender, dataset, agegroup) VALUES (''' || subjectcodeide ||  ''',''' || subjectvisitide || ''',''' || subjectvisitdate || ''',''' || gender || ''',''' || dataset_const || ''',' || agegroup || ')');
	ELSIF subjectageyears IS NULL AND subjectage IS NOT NULL THEN
		execute format('insert into ' || table_name  || '( subjectcode, subjectvisitid, subjectvisitdate, subjectage, gender, dataset, agegroup) VALUES (''' || subjectcodeide || ''',''' || subjectvisitide || ''',''' || subjectvisitdate || ''',' || subjectage || ',''' || gender || ''',''' || dataset_const || ''',' || agegroup || ')');
	ELSIF subjectageyears IS NOT NULL AND subjectage IS NULL THEN
		execute format('insert into ' || table_name  || '( subjectcode, subjectvisitid, subjectvisitdate, subjectageyears, gender, dataset, agegroup) VALUES (''' || subjectcodeide || ''',''' || subjectvisitide || ''',''' || subjectvisitdate || ''',' || subjectageyears || ',''' || gender || ''',''' || dataset_const || ''',' || agegroup || ')');
	ELSE
		execute format('insert into ' || table_name  || '( subjectcode, subjectvisitid, subjectvisitdate, subjectageyears, subjectage, gender, dataset, agegroup) VALUES (''' || subjectcodeide || ''',''' || subjectvisitide || ''',''' || subjectvisitdate || ''',' || subjectageyears || ',' || subjectage || ',''' || gender || ''',''' || dataset_const || ''',' || agegroup || ')');
	END IF;
	
	countInserts:=countInserts+1;
	
END LOOP;
-- Demographics info has been stored
raise notice 'countInserts: %', countInserts;
--raise notice 'countLoops: %', countLoops;
-- Store data. For every concept_cd and  <----------------------------


for concept in SELECT column_name FROM information_schema.columns WHERE information_schema.columns.table_name = 'new_table'
loop
	FOR subjectcodeide, encounter, encounter_ide, valtype, char_val, num_val IN
	  (SELECT em.patient_ide, obf.encounter_num, em.encounter_ide, obf.valtype_cd, obf.tval_char, obf.nval_num 
	   FROM observation_fact obf, encounter_mapping em
	   WHERE obf.concept_cd=concept AND obf.encounter_num=em.encounter_num)
	LOOP
		--raise notice 'valtype - char_val - num_val -patient_ide(subjectcodeide)- -encounter-: % - % - % -%- -%-', valtype, char_val, num_val, subjectcodeide, encounter;
		--raise notice 'concept: %', concept;
		--EXECUTE FORMAT('SELECT "'|| concept ||'" FROM '||table_name||' WHERE subjectvisitid = '' || encounter_ide || ''') INTO alreadyinserted;
		--IF alreadyinserted!='' AND alreadyinserted IS NOT NULL THEN CONTINUE; END IF;
		--raise notice 'Going to write a clinical variable........ alreadyinserted was %', alreadyinserted;
		CASE
		    WHEN 'T'=valtype -- the problem is when valtype is NOT T and num_val IS NULL.....
		    THEN
			IF char_val IS NULL THEN
				char_val = '';
			END IF;
			
			execute format('update ' || table_name  || ' set ' || '"' || concept || '" = ''' ||  char_val || ''' where subjectvisitid = ''' || encounter_ide || '''' );--many rows per patient -each one for every visit of her-
			countUpdates=countUpdates+1;
			--raise notice '--- Updated table % variable % = % for PAtient % for encounter %', table_name, concept, char_val, subjectcodeide, encounter_ide;
		    WHEN 'N'=valtype
		    THEN	
			IF num_val IS NOT NULL THEN
				execute format('UPDATE ' || table_name  || ' set ' || '"' || concept || '" = ' ||  num_val || ' where subjectvisitid = ''' || encounter_ide || '''' );
			countUpdates=countUpdates+1;
			--raise notice '--- Updated table % variable % = % for PAtient % for encounter %', table_name, concept, num_val, subjectcodeide, encounter_ide;
			END IF;		
			
		END CASE;
	end loop;
end loop;

raise notice 'CountUpdates: %', countUpdates;
--countUpdates := execute format('SELECT COUNT(*) FROM ' || table_name);
--raise notice 'new_table has % tuples', countUpdates;

BEGIN
	COPY new_table FROM '/tmp/harmonized_clinical_data.csv' DELIMITER ',' CSV HEADER ; 
EXCEPTION
WHEN OTHERS 
        THEN raise notice 'No such file /tmp/harmonized_clinical_data.csv';
END;
COPY (SELECT * FROM new_table) TO '/tmp/harmonized_clinical_data.csv' WITH CSV DELIMITER ',' HEADER;
EXECUTE FORMAT('DROP TABLE IF EXISTS temp_table');
EXECUTE FORMAT('DROP TABLE IF EXISTS ' || table_name);
END; $BODY$ language plpgsql;
select pivotfunction_long_fed();
