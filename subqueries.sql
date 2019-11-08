--Find the states which has over 20 candidates whose committee has given a negative amount of money in 2016
select state, Num_Cand
from
(select cand.state as state, count(cand.Name) as Num_Cand
from fec_modeled.Candidates_Beam_DF cand
where cand.Label in
(select Candidate_label
from fec_modeled.Contributes_Beam_DF
where (Year = 2016) and (Amount < 0))
group by cand.state
order by count(cand.Name) desc)
where Num_Cand >= 20

--Find the names of candidates who has not been a national candidate before 2020
select cand.Name
from fec_modeled.Candidates_Beam_DF cand
where cand.Year not in
(select distinct(Year)
from fec_modeled.Candidates_Beam
where Year < 2020)


--select the names of the committees in Texas which makes a contribution in Year 2018 higher than the average cotribution a committee made in 2018 nationwide. 

select com.name
from fec_modeled.Committees_Beam_DF com 
inner join fec_modeled.Contributes_Beam_DF con
on con.Committee_Label = com.Label
where com. State = "TX" and com.Year = 2018 and con.Amount > (
select avg(con.Amount)
from fec_modeled.Contributes_Beam_DF con
where con.Year = 2018)

--select the cities and group by the amount of times the committees in that city that contributed to Donald Trump in 2016

select com.city, count(com.Label)
from fec_modeled.Committees_Beam_DF com
where exists
(select *
from fec_modeled.Contributes_Beam_DF con 
inner join fec_modeled.Candidates_Beam_DF can
on can.Label = con.Candidate_Label
where com.Label = con.Committee_Label and can.Name like "%TRUMP, DONALD J%" and con.Year = 2016 and con.Transaction_Type in ('24E', '24K', '24Z'))
group by com.city
order by count(com.Label) DESC
