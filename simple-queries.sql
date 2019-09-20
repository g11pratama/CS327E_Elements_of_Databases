--list all records with negative transaction amounts in years 2009-2010, including all columns
select * 
from fec_staging.Committee_Contributions_to_Candidates_2009_2010 ten
where ten.TRANSACTION_AMT <= 0
order by ten.TRANSACTION_AMT, ten.CMTE_ID

--list zip codes and transaction amount from 2011-2012
select ZIP_CODE, TRANSACTION_AMT
from fec_staging.Committee_Contributions_to_Candidates_2011_2012 twelve
where twelve.TRANSACTION_AMT >= 5000
order by TRANSACTION_AMT

--list names of committees and transaction amount from 2013-2014 where the contribution is greater than zero but smaller than $100
select NAME, TRANSACTION_AMT
from fec_staging.Committee_Contributions_to_Candidates_2013_2014
where 0 < TRANSACTION_AMT and TRANSACTION_AMT < 100
order by TRANSACTION_AMT


--select contributions from committees in Austin, showing committee name and contribution amount
--note that since the the TRANSACTION_AMT datatype is string, descending ordering compares string values
select NAME, TRANSACTION_AMT
from fec_staging.Committee_Contributions_to_Candidates_2015_2016
where CITY = "Austin" or CITY = "AUSTIN"
order by TRANSACTION_AMT desc

--select records whose committee type is a Political Action Committee (PAC), showing committee name and contribution amount
select NAME, TRANSACTION_AMT
from fec_staging.Committee_Contributions_to_Candidates_2017_2018
where ENTITY_TP = "PAC"
order by NAME

--list names of committees and transaction amount from 2019-2020 where the contribution is greater $5000, ordered decreasingly by transaction amount
select NAME, TRANSACTION_AMT
from fec_staging.Committee_Contributions_to_Candidates_2019_2020
where TRANSACTION_AMT > 5000
order by TRANSACTION_AMT desc



Select Name
from fec_staging.Committee_Contributions_to_Candidates_2007_2008
where city = "WASHINGTON"
order by name asc

-- list all the political committees names in Washington DC during 2007 and 2008


Select CMTE_NM
from fec_staging.Committee_Master_2007_2008
where CMTE_TP = 'H'
order by CMTE_NM asc

-- list all the committees that raise money for the candidates of the U.S. House of Representatives in 2007-2008

Select TRES_NM
from fec_staging.Committee_Master_2009_2010
where CMTE_ST = 'TX'
order by TRES_NM desc

-- list all the Texas committee treasurers during 2009 and 2010

Select CMTE_NM
from fec_staging.Committee_Master_2011_2012
where CMTE_DSGN = 'J'
order by CMTE_NM desc

-- list all the committees that are designated as joint fundraisers in 2011 and 2012


Select CMTE_NM, CMTE_ZIP
from fec_staging.Committee_Master_2013_2014
where CMTE_CITY = 'AUSTIN'
order by CMTE_ZIP  asc

-- list all the committees's name and zipcode in Austin during 2013 and 2014

Select CMTE_ID, CMTE_NM
from fec_staging.Committee_Master_2015_2016
where CMTE_FILING_FREQ = 'M'
order by CMTE_NM  asc

-- list all the names and IDs of committees that raise money every month during 2015 to 2016 with ascending order of the committee's names.

Select CMTE_NM, CMTE_ST1, CMTE_ST2, CMTE_CITY, CMTE_ST
from fec_staging.Committee_Master_2017_2018
where CMTE_ST1 like '%PO BOX%'
order by CMTE_NM  desc

-- list all the names and addresses of committees that use POX box in their addresses during 2017 to 2018 with descending order of the committee's names.

Select CMTE_ID, CMTE_NM
from fec_staging.Committee_Master_2019_2020
where ORG_TP = 'T' and CMTE_ST = 'TX'
order by CMTE_ID asc

-- list all the IDs and names of committees that are organized by Trade associations and locate in Texas during 2019 to 2020 with ascending order of the committee's IDs.

