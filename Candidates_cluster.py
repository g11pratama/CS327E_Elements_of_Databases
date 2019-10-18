import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# If the zip code has more than five digits, just keep the first five digits
class ZIPFormatFn(beam.DoFn):
    def process(self, element):
        record = element
        zip = record.get('ZIP')
        
        
        if not zip is None:
            
            zip = str(zip)
            
            if len(zip) > 5:
                zip = zip[0:5]
            
            zip = int(zip)
            
        record["ZIP"] = zip
            
        return [record]


PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-contributes',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 4
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options = opts) as p:
    
    #Because of the time limits, we only extract the first 100 rows of data.
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query = 'SELECT * FROM fec_modeled.Candidates'))

    new_pcoll = query_results | 'Change zip code' >> beam.ParDo(ZIPFormatFn())
    
    dataset_id = 'fec_modeled'
    table_id = 'Candidates_Beam_DF'
    schema_id = 'ID:STRING, Name:STRING, Party:STRING, Election_Year:INTEGER, Office_State:STRING, District:INTEGER, Challenge_Status:STRING, Street1:STRING, Street2:STRING, City:STRING, State:STRING, ZIP:INTEGER, Label:STRING, Year:INTEGER'

    # write PCollection to new BQ table
    new_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                    table=table_id, 
                                                    schema=schema_id,
                                                    project=PROJECT_ID,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                    batch_size=int(100))
    result = p.run()
    result.wait_until_finish()