--Lists the number of committees listed in the Committee Master, grouped by year
select count(*) as Num_Committes, Year
from fec_modeled.Committees_Beam_DF
group by Year
order by Year

--Find the avg, min, max, and number of expenditures/contributions for or against Trump in 2016
select avg(Amount) as Average, min(Amount) as Min, max(Amount) as Max, count(*) as Num_Contributions
from fec_modeled.Contributes_Beam_DF
where Candidate_Label = '2016P80001571' and Amount > 0

-- find out the states in the U.S. that has more than 200 committees which contribute more than 2000 in year 2018

select count(com.Label) as Num_of_Committees, com.State
from fec_modeled.Committees_Beam_DF com 
join fec_modeled.Contributes_Beam_DF con on com.Label = con.Committee_Label
where con.Amount > 200 and con.Year = 2018
group by com.State
having count(com.Label) > 200
order by count(com.Label) DESC



-- find out committees in Texas which contributes to more than one Candidate in 2018

select count(con.Candidate_Label) as Num_of_Candidates, com.Name
from fec_modeled.Contributes_Beam_DF con
join fec_modeled.Committees_Beam_DF com
on con.Committee_Label = com.Label
where con.year = 2018
and com.State = "TX"
group by com.Name
having count(con.Candidate_Label) > 1
order by count(con.Candidate_Label) DESC


-- find out the average amount of each contribution for committees that contributes to more than 100 candidates in year 2016

select avg(con.Amount) as Avg_of_Contribution,count(con.Candidate_Label) as Num_of_Candidates, com.Name
from fec_modeled.Contributes_Beam_DF con
join fec_modeled.Committees_Beam_DF com
on con.Committee_Label = com.Label
where con.year = 2016
group by com.Name
having count(con.Candidate_Label) > 100
order by avg(con.Amount) DESC


-- find out the top 10 candidates who receive the most contribution in year 2016

select sum(con.Amount) as Total_Received_Contribution, can.Name 
from fec_modeled.Contributes_Beam_DF con
join fec_modeled.Candidates_Beam_DF can
on con.Candidate_Label = can.Label
where con.year = 2016
group by can.Name
order by sum(con.Amount) DESC
limit 10
