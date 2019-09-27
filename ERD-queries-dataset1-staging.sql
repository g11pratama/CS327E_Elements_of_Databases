select count(distinct SUB_ID)
from fec_staging.Committee_Contributions_to_Candidates_2007_2008

select count(distinct SUB_ID)
from fec_staging.Committee_Contributions_to_Candidates_2009_2010

select count(distinct SUB_ID)
from fec_staging.Committee_Contributions_to_Candidates_2011_2012

select count(distinct SUB_ID)
from fec_staging.Committee_Contributions_to_Candidates_2013_2014

select count(distinct SUB_ID)
from fec_staging.Committee_Contributions_to_Candidates_2015_2016

select count(distinct SUB_ID)
from fec_staging.Committee_Contributions_to_Candidates_2017_2019

select count(distinct SUB_ID)
from fec_staging.Committee_Contributions_to_Candidates_2019_2020



select count(cc.CMTE_ID)
from fec_staging.Committee_Contributions_to_Candidates_2007_2008 cc inner join fec_staging.Committee_Master_2007_2008 cm on cc.CMTE_ID = cm.CMTE_ID

select count(cc.CMTE_ID) 
from fec_staging.Committee_Contributions_to_Candidates_2009_2010 cc inner join fec_staging.Committee_Master_2009_2010 cm on cc.CMTE_ID = cm.CMTE_ID

select count(cc.CMTE_ID) 
from fec_staging.Committee_Contributions_to_Candidates_2011_2012 cc inner join fec_staging.Committee_Master_2011_2012 cm on cc.CMTE_ID = cm.CMTE_ID

select count(cc.CMTE_ID) 
from fec_staging.Committee_Contributions_to_Candidates_2013_2014 cc inner join fec_staging.Committee_Master_2013_2014 cm on cc.CMTE_ID = cm.CMTE_ID



select count(cc.CMTE_ID) 
from fec_staging.Committee_Contributions_to_Candidates_2015_2016 cc inner join fec_staging.Committee_Master_2015_2016 cm on cc.CMTE_ID = cm.CMTE_ID

--the result of the above query does not result in the number of rows in the contributions table; the count is smaller by one
--the below queries investigated which foreign key is the problem

select cc.CMTE_ID
from fec_staging.Committee_Contributions_to_Candidates_2015_2016 cc left outer join fec_staging.Committee_Master_2015_2016 cm on cc.CMTE_ID = cm.CMTE_ID
where cm.CMTE_ID is null

select *
from fec_staging.Committee_Contributions_to_Candidates_2015_2016
where CMTE_ID = 'C00455357'



select count(cc.CMTE_ID) 
from fec_staging.Committee_Contributions_to_Candidates_2017_2018 cc inner join fec_staging.Committee_Master_2017_2018 cm on cc.CMTE_ID = cm.CMTE_ID

select count(cc.CMTE_ID) 
from fec_staging.Committee_Contributions_to_Candidates_2019_2020 cc inner join fec_staging.Committee_Master_2019_2020 cm on cc.CMTE_ID = cm.CMTE_ID


select count(distinct CMTE_ID)
from fec_staging.Committee_Master_2007_2008

select count(distinct CMTE_ID)
from fec_staging.Committee_Master_2009_2010

select count(distinct CMTE_ID)
from fec_staging.Committee_Master_2011_2012

select count(distinct CMTE_ID)
from fec_staging.Committee_Master_2013_2014

select count(distinct CMTE_ID)
from fec_staging.Committee_Master_2015_2016


--the resulting count of this query is two smaller than the number rows of the table
select count(distinct CMTE_ID)
from fec_staging.Committee_Master_2017_2018

-- same problem as above; the resulting count of this query is two smaller than the number rows of the table
select count(distinct CMTE_ID)
from fec_staging.Committee_Master_2019_2020
