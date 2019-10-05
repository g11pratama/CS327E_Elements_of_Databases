create Table fec_modeled.committee_2019_2020 as
select CMTE_ID as ID, CMTE_NM as Name, TRES_NM as Treasurer, CMTE_ST1 as Steet1, CMTE_ST2 as Street2, CMTE_CITY as City, CMTE_ST as State, safe_cast(CMTE_ZIP as INT64) as ZIP, CMTE_DSGN as Designation, CMTE_TP as Type, CMTE_PTY_AFFILIATION as Party_Affiliation, CMTE_FILING_FREQ as Filing_Frequency, ORG_TP as Category
from fec_staging.Committee_Master_2019_2020

create table fec_modeled.candidates_2019_2020 as
select CAND_ID AS ID, CAND_NAME AS Name, CAND_PTY_AFFILIATION AS Party, CAND_ELECTION_YR AS Election_Year, CAND_OFFICE_ST AS Office_State, CAND_OFFICE_DISTRICT as District, CAND_ICI as Challenge_Status, CAND_ST1 as Street1, CAND_ST2 as Street2, CAND_CITY as City, CAND_ST as State, CAND_ZIP as ZIP
from fec_staging.Candidates_Master_2020

create Table fec_modeled.supports_2019_2020 as
select CMTE_ID as Committee_ID, CAND_ID as Candidate_ID
from fec_staging.Committee_Master_2019_2020

create table fec_modeled.contributes_2019_2020 as
select CMTE_ID as Committee_ID, CAND_ID as Candidate_ID, TRANSACTION_TP as Transaction_Type, TRANSACTION_DT as Date, TRANSACTION_AMT as Amount, OTHER_ID as Payee_ID, SUB_ID as Contribution_ID
from fec_staging.Committee_Contributions_to_Candidates_2019_2020

select count(distinct Committee_ID)
from fec_modeled.supports_2007_2008

select count(distinct ID)
from fec_modeled.candidates_2007_2008

select count(distinct Contribution_ID)
from fec_modeled.contributes_2007_2008

select count(distinct ID)
from fec_modeled.committee_2007_2008

--delete the extra field name rows
delete from fec_modeled.contributes_2015_2016
where Committee_ID = 'CMTE_ID'

--gives error
select cast(CMTE_ZIP as INT64) as Zip
from fec_modeled.committee_2015_2016

-- Query used to investigate why an inner join on a foreign key from one table does not result in the same amount records as that same table
select cc.Committee_ID
from fec_modeled.contributes_2015_2016 cc left outer join fec_modeled.committee_2015_2016 cm on cc.Committee_ID = cm.ID
where cm.ID is null