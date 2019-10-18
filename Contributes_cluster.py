import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Add a dashline between the year, the month and the day. Also move the year from the end of the string to the begining of the string.
class DateFormatFn(beam.DoFn):
    def process(self, element):
        record = element
        date = record.get('Date')

        if date is not None:
            if len(date) == 7:
                date = '0' + date

            date = date[4:] + '-' + date[0:2] + '-' + date[2:4]
            record["Date"] = date
        
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
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query = 'SELECT * FROM fec_modeled.Contributes'))


    new_pcoll = query_results | 'Change date format' >> beam.ParDo(DateFormatFn())


    dataset_id = 'fec_modeled'
    table_id = 'Contributes_Beam_DF'
    schema_id = 'Transaction_Type:STRING, Date:DATE, Amount:INTEGER, Payee_ID:STRING, Contribution_ID:STRING, Year:INTEGER, Committee_Label:STRING, Candidate_Label:STRING'

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