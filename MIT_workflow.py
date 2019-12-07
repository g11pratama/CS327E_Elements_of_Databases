import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 12, 06)
}

staging_dataset = 'MIT_workflow_staging'
intermediate_dataset = 'MIT_workflow_intermediate'
modeled_dataset = 'MIT_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

create_int_elect_sql = 'create or replace table ' + intermediate_dataset + '''.Elections as
((select year as Year, state_po as State, "House" as Type, District, stage as Stage, runoff as Runoff, special as Special, totalvotes as Total_Votes 
                      from ''' + staging_dataset + '''.House_Elections) 
                     union all 
(select year as Year, state_po as State, "President" as Type, null as District, null as Stage, null as Runoff, null as Speacial, totalvotes as Total_Votes 
                      from ''' + staging_dataset + '''.President_Elections) 
                      union all 
(select year as Year, state_po as State, "Senate" as Type, null as District, stage as Stage, null as Runoff, special as Speacial, totalvotes as Total_Votes
                      from ''' + staging_dataset + '.Senate_Elections))'

create_elect_pk_sql = 'create or replace table ' + intermediate_dataset + '''.Elections_with_PK as
(
select 
CONCAT(cast(Year as string), State, Type, if(District is Null, "NA", cast(District as string)), if(Stage is Null, "NA", Stage)) as Election_ID, Year, State, Type, District, Stage, Runoff, Special, Total_Votes from ''' + intermediate_dataset + '.Elections)'

create_elect_distinct_pk_sql = 'create or replace table ' + intermediate_dataset + '''.Elections_Distinct_PK as
(select distinct * from ''' + intermediate_dataset + '.Elections_with_PK)'

create_modeled_elect_sql = 'create or replace table ' + modeled_dataset + '''.Elections as
(select Election_ID, Year, State, Type, District, Stage, cast(if( Runoff = "NA", null, Runoff) as Bool) as Runoff, Special, cast(TRUNC( Total_Votes ) as int64) as Total_Votes
from ''' + intermediate_dataset + '.Elections_Distinct_PK)'

create_modeled_runs_sql = 'create or replace table ' + modeled_dataset + '''.Runs as
(select Candidate, Candidate_Votes, CONCAT(cast(Year as string), State, Type, if(District is Null, "NA", cast(District as string)), if(Stage is Null, "NA", cast(Stage as string))) as Election_ID
from(
(select candidate as Candidate, candidatevotes as Candidate_Votes, year as Year, state_po as State, "House" as Type, District, stage as Stage, totalvotes as Total_Votes
from ''' + staging_dataset + '''.House_Elections)
union all 
(select candidate as Candidate, candidatevotes as Candidate_Votes, year as Year, state_po as State, "President" as Type, null as District, null as Stage, totalvotes as Total_Votes
from ''' + staging_dataset + '''.President_Elections)
union all 
(select candidate as Candidate, candidatevotes as Candidate_Votes, year as Year, state_po as State, "Senate" as Type, null as District, state as Stage, totalvotes as Total_Votes
from ''' + staging_dataset + '''.Senate_Elections)) where Candidate != "NA")'''

create_prefinal_modeled_runs_sql = 'create or replace table ' + modeled_dataset + '''.Runs as
(select distinct *
from ''' + modeled_dataset +  '''.Runs
where SAFE.SUBSTR(Election_ID, 0, 4) >= "2008")'''

create_final_modeled_elect_sql = 'create or replace table ' + modeled_dataset + '''.Elections as
(select distinct *
from ''' + modeled_dataset +  '''.Elections
where SAFE.SUBSTR(Election_ID, 0, 4) >= "2008")'''

with models.DAG(
        'MIT_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    
    create_staging_dataset = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_intermediate_dataset = BashOperator(
            task_id='create_intermediate_dataset',
            bash_command='bq --location=US mk --dataset ' + intermediate_dataset)
    
    create_modeled_dataset = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    split = DummyOperator(
            task_id='split',
            trigger_rule='all_done')
    
    load_president = BashOperator(
            task_id='load_president',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.President_Elections \
                         "gs://cs327efall2019_electionresults/President Data.csv"',
            trigger_rule = 'dummy')
    
    load_senate = BashOperator(
            task_id='load_senate',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Senate_Elections \
                         "gs://cs327efall2019_electionresults/Senate.csv"',
            trigger_rule = 'dummy')
    
    load_house = BashOperator(
            task_id='load_house',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.House_Elections \
                         "gs://cs327efall2019_electionresults/house.csv"',
            trigger_rule = 'dummy')
    
    create_int_elections = BashOperator(
            task_id='create_elections',
            bash_command=bq_query_start + "'" + create_int_elect_sql + "'", 
            trigger_rule='one_success')
    
    create_elect_pk = BashOperator(
            task_id='create_election_with_pk',
            bash_command=bq_query_start + "'" + create_elect_pk_sql + "'", 
            trigger_rule='one_success')
    
    create_elect_distinct_pk = BashOperator(
            task_id='create_elections_with_distict_pk', 
            bash_command=bq_query_start + "'" + create_elect_distinct_pk_sql + "'", 
            trigger_rule='one_success')
    
    create_modeled_elect = BashOperator(
            task_id='create_modeled_elect', 
            bash_command=bq_query_start + "'" + create_modeled_elect_sql + "'", 
            trigger_rule='one_success')
    
    create_modeled_runs = BashOperator(
            task_id='create_modeled_runs', 
            bash_command=bq_query_start + "'" + create_modeled_runs_sql + "'", 
            trigger_rule='one_success')
    
    runs_dataflow = BashOperator(
            task_id='runs_dataflow',
            bash_command='python /home/jupyter/airflow/dags/transform_Runs_cluster.py')
    
    create_prefinal_modeled_runs = BashOperator(
            task_id='create_prefinal_modeled_runs', 
            bash_command=bq_query_start + "'" + create_prefinal_modeled_runs_sql + "'", 
            trigger_rule='one_success')
    
    create_final_modeled_elect = BashOperator(
            task_id='create_final_modeled_elections', 
            bash_command=bq_query_start + "'" + create_final_modeled_elect_sql + "'", 
            trigger_rule='one_success')
    
    
    create_staging_dataset >> create_intermediate_dataset >> create_modeled_dataset >> [load_president, load_senate, load_house] >> create_int_elections >> create_elect_pk >> create_elect_distinct_pk >> split    
    split >> create_modeled_elect >> create_final_modeled_elect
    split >> create_modeled_runs >> create_prefinal_modeled_runs >> runs_dataflow
    