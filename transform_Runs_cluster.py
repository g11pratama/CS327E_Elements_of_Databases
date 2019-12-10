import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class NameFormatFn(beam.DoFn):
    def process(self, element):
        record = element
        name = record.get('Candidate')
        name = name.upper()
        name = name.replace("\"","")
        name = name.replace("\\","")
        name = name.replace(', JR.', '')
        name = name.replace(', SR.', '')
        name = name.replace(', II', '')
        name = name.replace(', III', '')
        name = name.replace(', JR', '')
        name = name.replace(', SR', '')
        name = name.replace(' JR', '')
        name = name.replace(' SR', '')
        name = name.replace(' II', '')
        name = name.replace(' III', '')
        name = name.replace('.','')
        
        if "," in name:
            name = name.split()
            record['Candidate'] = name[0] + " " + name[1]
        else:
            name = name.split()
            if len(name) == 1:
                record['Candidate'] = name[0]
            else:
                record['Candidate'] = name[-1] + ', ' + name[0]
                
        return [record]
    
class RunsNamePreGroupFn(beam.DoFn):
    def process(self, element):
        record = element
        name = record.pop('Candidate')
        
        return [(name, record)]
    
class CandNamePreGroupFn(beam.DoFn):
    def process(self, element):
        record = element
        name = record.pop('Name')
        
        return[(name, record)]
    
class RemoveEmptyRunsFn(beam.DoFn):
    def process(self, element):
        name, record = element
        
        if len(record['Runs']) == 0 or len(record['Cand']) == 0:
            pass
        
        else:
            return [element]
        
class CreatePreFinalRecordsFn(beam.DoFn):
    def process(self, element):
        name, record_dict = element
        record_lst = []
        runsKey_lst = []
        
        for x in record_dict['Runs']:
            for y in record_dict['Cand']:
                cand_dict = {'Election_ID':x['Election_ID'],'Candidate_Votes':x['Candidate_Votes']}
                cand_dict['Candidate_Label'] = cand_dict['Election_ID'][:4] + y['ID']
                runsKey = cand_dict['Candidate_Label'] + cand_dict['Election_ID'][4:]
                cand_dict['Runs_ID'] = runsKey
                
                if runsKey not in runsKey_lst:
                    runsKey_lst.append(runsKey)
                    record_lst.append(cand_dict)
            
        return record_lst
    

def run():
    PROJECT_ID = 'sound-cider-252823'
    BUCKET = 'gs://gerryandhao'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    options = {
        'runner': 'DataflowRunner',
        'job_name': 'transform-runs',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
        'num_workers': 4
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)
    
    with beam.Pipeline('DataflowRunner', options = opts) as p:
        query_results = p | 'Read from BigQuery for txt' >> beam.io.Read(beam.io.BigQuerySource(query = 'SELECT * FROM MIT_workflow_modeled.Runs'))
        cand_query_results = p | 'Read Candidates_Beam_DF_Jupyter from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query = 'SELECT Name, ID from fec_modeled.Candidates_Beam_DF_Jupyter'))

        #perform name standardization by calling the NameFormat ParDo
        new_pcoll = query_results | 'Perform name standardization' >> beam.ParDo(NameFormatFn())

        #pregrouping for runs
        runs_name_pcoll = new_pcoll | 'Runs pregroup processing' >> beam.ParDo(RunsNamePreGroupFn())


        #pregrouping for candidates table
        cand_name_pcoll = cand_query_results | 'Cand pregroup processing' >> beam.ParDo(CandNamePreGroupFn())

        #perform cogroupbykey
        group_pcoll = {'Runs':runs_name_pcoll,'Cand':cand_name_pcoll} | 'Grouping' >> beam.CoGroupByKey()

        #removing empties
        cleaned_group_pcoll = group_pcoll | "Remove empty runs record" >> beam.ParDo(RemoveEmptyRunsFn())

        #create near final records
        pre_final_pcoll = cleaned_group_pcoll | "Create near final records" >> beam.ParDo(CreatePreFinalRecordsFn())


        dataset_id = 'MIT_workflow_modeled'
        table_id ='Runs_Beam_DF_Jupyter'
        schema_id = 'Runs_ID:STRING, Candidate_Label:STRING, Candidate_Votes:INTEGER, Election_ID:STRING'

        # write the output results as a table in BigQuery
        pre_final_pcoll | 'Write to BigQuery' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                        table=table_id, 
                                                        schema=schema_id,
                                                        project=PROJECT_ID,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                        batch_size=int(100))

        result = p.run()
        result.wait_until_finish()
        
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()