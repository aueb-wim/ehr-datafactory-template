CREATE or replace function pivotfunction_fed() RETURNS void AS $BODY$
DECLARE concept text;
DECLARE valtype text;
DECLARE type_name text;
DECLARE query text;
DECLARE table_name text;
DECLARE char_val text;
DECLARE num_val numeric(18,5);
DECLARE usedid text;
DECLARE currentid text;
DECLARE sex text;
DECLARE year integer;
DECLARE startdate timestamp without time zone;
DECLARE subjectcode text;
DECLARE subjectageyears integer;
DECLARE subjectage numeric(18,5);
DECLARE gender text;
DECLARE dataset text;
DECLARE agegroup text;-- in this pivotfunction version agegroup s value is still calculated here (considering the harmonized i2b2 does NOT have it).. in the next version (NEW2) we will consider that the harmonized i2b2 already has calculated it
DECLARE encounter text;
DECLARE countUpdates integer;
DECLARE countInserts integer;
BEGIN
query := '';
usedid := '';
countUpdates := 0;
countInserts := 0;
-- create table for cde variables - the list of variables in this table is fixed (cde variables)
table_name := 'new_table';
query := 'CREATE TABLE ' || table_name || '( "subjectcode" text, "subjectageyears" text, "subjectage" text, "gender" text, "_3rdventricle" text, "_4thventricle" text, "rightaccumbensarea" text, "leftaccumbensarea" text, "rightamygdala" text, "leftamygdala" text, "brainstem" text, "rightcaudate" text, "leftcaudate" text, "rightcerebellumexterior" text, "leftcerebellumexterior" text, "rightcerebellumwhitematter" text, "leftcerebellumwhitematter" text, "rightcerebralwhitematter" text, "leftcerebralwhitematter" text, "csfglobal" text, "righthippocampus" text, "lefthippocampus" text, "rightinflatvent" text, "leftinflatvent" text, "rightlateralventricle" text, "leftlateralventricle" text, "rightpallidum" text, "leftpallidum" text, "rightputamen" text, "leftputamen" text, "rightthalamusproper" text, "leftthalamusproper" text, "rightventraldc" text, "leftventraldc" text, "opticchiasm" text, "cerebellarvermallobulesiv" text, "cerebellarvermallobulesvivii" text, "cerebellarvermallobulesviiix" text, "leftbasalforebrain" text, "rightbasalforebrain" text, "rightacgganteriorcingulategyrus" text, "leftacgganteriorcingulategyrus" text, "rightainsanteriorinsula" text, "leftainsanteriorinsula" text, "rightaorganteriororbitalgyrus" text, "leftaorganteriororbitalgyrus" text, "rightangangulargyrus" text, "leftangangulargyrus" text, "rightcalccalcarinecortex" text, "leftcalccalcarinecortex" text, "rightcocentraloperculum" text, "leftcocentraloperculum" text, "rightcuncuneus" text, "leftcuncuneus" text, "rightententorhinalarea" text, "leftententorhinalarea" text, "rightfofrontaloperculum" text, "leftfofrontaloperculum" text, "rightfrpfrontalpole" text, "leftfrpfrontalpole" text, "rightfugfusiformgyrus" text, "leftfugfusiformgyrus" text, "rightgregyrusrectus" text, "leftgregyrusrectus" text, "rightioginferioroccipitalgyrus" text, "leftioginferioroccipitalgyrus" text, "rightitginferiortemporalgyrus" text, "leftitginferiortemporalgyrus" text, "rightliglingualgyrus" text, "leftliglingualgyrus" text, "rightlorglateralorbitalgyrus" text, "leftlorglateralorbitalgyrus" text, "rightmcggmiddlecingulategyrus" text, "leftmcggmiddlecingulategyrus" text, "rightmfcmedialfrontalcortex" text, "leftmfcmedialfrontalcortex" text, "rightmfgmiddlefrontalgyrus" text, "leftmfgmiddlefrontalgyrus" text, "rightmogmiddleoccipitalgyrus" text, "leftmogmiddleoccipitalgyrus" text, "rightmorgmedialorbitalgyrus" text, "leftmorgmedialorbitalgyrus" text, "rightmpogpostcentralgyrusmedialsegment" text, "leftmpogpostcentralgyrusmedialsegment" text, "rightmprgprecentralgyrusmedialsegment" text, "leftmprgprecentralgyrusmedialsegment" text, "rightmsfgsuperiorfrontalgyrusmedialsegment" text, "leftmsfgsuperiorfrontalgyrusmedialsegment" text, "rightmtgmiddletemporalgyrus" text, "leftmtgmiddletemporalgyrus" text, "rightocpoccipitalpole" text, "leftocpoccipitalpole" text, "rightofugoccipitalfusiformgyrus" text, "leftofugoccipitalfusiformgyrus" text, "rightopifgopercularpartoftheinferiorfrontalgyrus" text, "leftopifgopercularpartoftheinferiorfrontalgyrus" text, "rightorifgorbitalpartoftheinferiorfrontalgyrus" text, "leftorifgorbitalpartoftheinferiorfrontalgyrus" text, "rightpcggposteriorcingulategyrus" text, "leftpcggposteriorcingulategyrus" text, "rightpcuprecuneus" text, "leftpcuprecuneus" text, "rightphgparahippocampalgyrus" text, "leftphgparahippocampalgyrus" text, "rightpinsposteriorinsula" text, "leftpinsposteriorinsula" text, "rightpoparietaloperculum" text, "leftpoparietaloperculum" text, "rightpogpostcentralgyrus" text, "leftpogpostcentralgyrus" text, "rightporgposteriororbitalgyrus" text, "leftporgposteriororbitalgyrus" text, "rightppplanumpolare" text, "leftppplanumpolare" text, "rightprgprecentralgyrus" text, "leftprgprecentralgyrus" text, "rightptplanumtemporale" text, "leftptplanumtemporale" text, "rightscasubcallosalarea" text, "leftscasubcallosalarea" text, "rightsfgsuperiorfrontalgyrus" text, "leftsfgsuperiorfrontalgyrus" text, "rightsmcsupplementarymotorcortex" text, "leftsmcsupplementarymotorcortex" text, "rightsmgsupramarginalgyrus" text, "leftsmgsupramarginalgyrus" text, "rightsogsuperioroccipitalgyrus" text, "leftsogsuperioroccipitalgyrus" text, "rightsplsuperiorparietallobule" text, "leftsplsuperiorparietallobule" text, "rightstgsuperiortemporalgyrus" text, "leftstgsuperiortemporalgyrus" text, "righttmptemporalpole" text, "lefttmptemporalpole" text, "righttrifgtriangularpartoftheinferiorfrontalgyrus" text, "lefttrifgtriangularpartoftheinferiorfrontalgyrus" text, "rightttgtransversetemporalgyrus" text, "leftttgtransversetemporalgyrus" text, "montrealcognitiveassessment" text, "minimentalstate" text, "agegroup" text, "handedness" text, "updrstotal" text, "updrshy" text, "adnicategory" text, "edsdcategory" text, "ppmicategory" text, "alzheimerbroadcategory" text, "parkinsonbroadcategory" text, "neurogenerativescategories" text, "dataset" text, "apoe4" text, "rs3818361_t" text, "rs744373_c" text, "rs190982_g" text, "rs1476679_c" text, "rs11767557_c" text, "rs11136000_t" text, "rs610932_a" text, "rs3851179_a" text, "rs17125944_c" text, "rs10498633_t" text, "rs3764650_g" text, "rs3865444_t" text, "rs2718058_g" text, "fdg" text, "pib" text, "av45" text';
-- collect all hospital specific concept_cds in order to obtain the available columns
raise notice '1. Here is the CREATE TABLE query: %', query;
/*for concept, valtype in 	-- UPDATE: commenting out this part... cause we said that in the anonymized federation dataset we ONLY want the CDEs...
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
	end if;
end if;
end loop;*/
raise notice '2. Here is the CREATE TABLE query: %', query;
-- add primary key
query := query || ', PRIMARY KEY (subjectcode))';
raise notice '3. And here it is now: %', query;
-- execute statement
execute format('DROP TABLE IF EXISTS ' || table_name);
--raise notice '4. And here it is now: %', query;
execute format(query);
-- Table has been created. Time to populate it. 

--Need to store information other than concept_cd, like subjectageyears, subjectage, gender, dataset, and agegroup
for subjectcode, subjectageyears, subjectage, gender, dataset in 
	select observation_fact.patient_num, round(visit_dimension.patient_age), visit_dimension.patient_age, patient_dimension.sex_cd, observation_fact.provider_id
	from	observation_fact, patient_dimension, visit_dimension, 
		( (select v.patient_num, v.encounter_num from (select patient_num, min(patient_age) minage from visit_dimension group by patient_num) as foo1,
		visit_dimension as v where foo1.patient_num = v.patient_num and foo1.minage = v.patient_age)
		UNION
		(select v.patient_num, v.encounter_num from (select patient_num, min(start_date) mindate from visit_dimension group by patient_num) as foo2,
		visit_dimension as v where foo2.patient_num = v.patient_num and foo2.mindate = v.start_date) ) as encounter_num_with_min_age_or_date
	where observation_fact.patient_num=patient_dimension.patient_num and encounter_num_with_min_age_or_date.encounter_num = observation_fact.encounter_num and 		
	encounter_num_with_min_age_or_date.encounter_num = visit_dimension.encounter_num and encounter_num_with_min_age_or_date.patient_num = observation_fact.patient_num
        order by patient_num
LOOP
currentid := ',' || subjectcode || ',';

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
			execute format('insert into ' || table_name  || '( subjectcode, gender, dataset, agegroup) VALUES (''' || subjectcode ||  ''',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		ELSIF subjectageyears IS NULL AND subjectage IS NOT NULL THEN
			execute format('insert into ' || table_name  || '( subjectcode, subjectage, gender, dataset, agegroup) VALUES (''' || subjectcode || ''','  || subjectage || ',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		ELSIF subjectageyears IS NOT NULL AND subjectage IS NULL THEN
			execute format('insert into ' || table_name  || '( subjectcode, subjectageyears, gender, dataset, agegroup) VALUES (''' || subjectcode || ''',' || subjectageyears || ',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		ELSE
		execute format('insert into ' || table_name  || '( subjectcode, subjectageyears, subjectage, gender, dataset, agegroup) VALUES (''' || subjectcode || ''',' || subjectageyears || ',' || subjectage || ',''' || gender || ''',''' || dataset || ''',' || agegroup || ')');
		usedid := usedid || ',' || subjectcode ||',';
		END IF;
		countInserts=countInserts+1;
	end if;
END LOOP;
-- Demographics info has been stored
raise notice 'countInserts: %', countInserts;
-- Store data. For every concept_cd and every patient store the data that are the OLDEST
for concept in SELECT column_name FROM information_schema.columns WHERE information_schema.columns.table_name = 'new_table'
loop
	for subjectcode, encounter, startdate, valtype, char_val, num_val in
	(select v.patient_num, v.encounter_num, v.start_date, v.valtype_cd, v.tval_char, v.nval_num 
	from 
		(select patient_num, min(start_date) mindate from observation_fact where concept_cd = concept group by patient_num) as foo,
		observation_fact as v 
	where foo.patient_num = v.patient_num and foo.mindate = v.start_date and concept_cd = concept)
	loop
		raise notice 'valtype - char_val - num_val -patient_ide(subjectcode)-: % - % - % -%-', valtype, char_val, num_val, subjectcode;
		CASE
		    WHEN 'T'=valtype -- the problem is when valtype is NOT T and num_val IS NULL..... 
		    THEN
			IF char_val IS NULL THEN
				char_val = '';
			END IF;
			execute format('update ' || table_name  || ' set ' || '"' || concept || '" = ''' ||  char_val || ''' where subjectcode = ''' || subjectcode || '''');		    
			countUpdates=countUpdates+1;
		    WHEN 'N'=valtype
		    THEN	
			IF num_val IS NOT NULL THEN
				execute format('UPDATE ' || table_name  || ' set ' || '"' || concept || '" = ' ||  num_val || ' where subjectcode = ''' || subjectcode || '''');
			countUpdates=countUpdates+1;
			END IF;		
			
		  --  ELSE			
			--execute format('UPDATE ' || table_name  || ' set ' || '"' || concept || '" = ' ||  NULL || ' where subjectcode = ' || subjectcode );			countUpdates=countUpdates+1;
		END CASE;
	end loop;
end loop;

raise notice 'CountUpdates: %', countUpdates;
--countUpdates := execute format('SELECT COUNT(*) FROM ' || table_name);
--raise notice 'new_table has % tuples', countUpdates;

BEGIN
	COPY new_table FROM '/tmp/harmonized_clinical_anon_data_.csv' DELIMITER ',' CSV HEADER ; 
EXCEPTION
WHEN OTHERS 
        THEN raise notice 'No such file /tmp/harmonized_clinical_anon_data.csv';
END;
COPY (SELECT * FROM new_table) TO '/tmp/harmonized_clinical_anon_data.csv' WITH CSV DELIMITER ',' HEADER;
EXECUTE FORMAT('DROP TABLE IF EXISTS ' || table_name);
END; $BODY$ language plpgsql;
select pivotfunction_fed();
