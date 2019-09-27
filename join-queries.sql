-- Lists transactions made by a commitee named "BERNIE 2020" and to whom it gave
select cm.CMTE_ID, cm.CMTE_DSGN, cm.CMTE_TP, cm.CMTE_NM, cc.CMTE_ID as DONATED_TO, cd.CMTE_NM as DONATED_NM, cd.CMTE_DSGN as CMTE_DES, cd.CMTE_TP as TYPE, cc.TRANSACTION_AMT, cc.TRANSACTION_DT
from fec_staging.Committee_Contributions_to_Candidates_2019_2020 cc, fec_staging.Committee_Master_2019_2020 cm, fec_staging.Committee_Master_2019_2020 cd
where cc.OTHER_ID = cm.CMTE_ID and cc.NAME = 'BERNIE 2020' and cc.CMTE_ID = cd.CMTE_ID

-- Query used to investigate why an inner join on a foreign key from one table does not result in the same amount records as that same table
select cc.CMTE_ID
from fec_staging.Committee_Contributions_to_Candidates_2015_2016 cc left outer join fec_staging.Committee_Master_2015_2016 cm on cc.CMTE_ID = cm.CMTE_ID
where cm.CMTE_ID is null

--Lists the names of the committees to which "JOHN.D.DINGELL FOR CONGRESS" donated
select cm.CMTE_NM, cc.NAME as donor
from fec_staging.Committee_Contributions_to_Candidates_2013_2014 cc inner join fec_staging.Committee_Master_2013_2014 cm on cc.CMTE_ID = cm.CMTE_ID
where cc.OTHER_ID = 'C00002600'

select distinct cm.CMTE_NM
from fec_staging.Committee_Contributions_to_Candidates_2007_2008 cc
full outer join fec_staging.Committee_Master_2007_2008 cm
on cc.CMTE_ID = cm.CMTE_ID
where cc. TRANSACTION_AMT > 10000

-- find all the committee's names which gave more than 10,000 USD to candidates in 2007 and 2008


select distinct cm.CMTE_NM
from fec_staging.Committee_Contributions_to_Candidates_2009_2010 cc
full outer join fec_staging.Committee_Master_2009_2010 cm
on cc.CMTE_ID = cm.CMTE_ID
where cc.TRANSACTION_TP = '24K'

-- find all the committee's names whose contribution are made to nonaffiliated committees in 2009 and 2010


select distinct cm.TRES_NM
from fec_staging.Committee_Contributions_to_Candidates_2011_2012 cc
full outer join fec_staging.Committee_Master_2011_2012 cm
on cc.CMTE_ID = cm.CMTE_ID
where cc. ENTITY_TP = 'PTY'

-- find the treasurer names for all party organizations in 2011 and 2012
