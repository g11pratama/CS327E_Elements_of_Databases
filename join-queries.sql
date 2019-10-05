---- Lists transactions made to a commitee named "BERNIE 2020" and by whom it gave
select cm.ID, cm.Name, cont.Candidate_ID, cont.Amount
from fec_modeled.committee_2019_2020 cm inner join fec_modeled.contributes_2019_2020 cont on cm.ID = cont.Committee_ID
where cont.Payee_ID = 'C00696948'


-- Query used to investigate why an inner join on a foreign key from one table does not result in the same amount records as that same table
select cc.Committee_ID
from fec_modeled.contributes_2015_2016 cc left outer join fec_modeled.committee_2015_2016 cm on cc.Committee_ID = cm.ID
where cm.ID is null

--Who donated to a committee called 'John D. Dingell For Congress"
select cm.Name as donor
from fec_modeled.contributes_2013_2014 cc inner join fec_modeled.committee_2013_2014 cm on cc.Committee_ID = cm.ID
where cc.Payee_ID = 'C00002600'




select distinct cm.Name
from fec_modeled.contributes_2007_2008 cc
full outer join fec_modeled.committee_2007_2008 cm
on cc.Committee_ID = cm_ID
where cc.Amount > 10000

-- find all the committee's names which gave more than 10,000 USD to candidates in 2007 and 2008

select distinct cm.CMTE_NM
from fec_modeled.contributes_2009_2010 cc
full outer join fec_modeled.committee_2009_2010 cm
on cc.Committee_ID = cm.ID
where cc.Type = '24K'

-- find all the committee's names whose contribution are made to nonaffiliated committees in 2009 and 2010


select distinct cm.TRES_NM
from fec_modeled.contributes_2011_2012 cc
full outer join fec_modeled.committee_2011_2012 cm
on cc.Committee_ID = cm.ID
where cc.Type = 'PTY'

-- find the treasurer names for all party organizations in 2011 and 2012