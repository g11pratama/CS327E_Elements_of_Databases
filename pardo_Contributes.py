import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Add a dashline between the year, the month and the day. Also move the year from the end of the string to the begining of the string.
class DateFormatFn(beam.DoFn):
    def process(self, element):
        record = element
        date = record.get('Date')

        if len(date) == 7:
            date = '0' + date

        date = date[4:] + '-' + date[0:2] + '-' + date[2:4]
        record["Date"] = date
        
        return [record]


PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
        'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options = opts) as p:
    
    #Because of the time limits, we only extract the first 100 rows of data.
    query_results_to_txt = p | 'Read from BigQuery for txt' >> beam.io.Read(beam.io.BigQuerySource(query = 'SELECT * FROM fec_modeled.Contributes limit 100'))
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query = 'SELECT * FROM fec_modeled.Contributes'))

    #write PCollection to log file
    query_results_to_txt | 'Write to input.txt' >> WriteToText('input.txt')

    new_pcoll_to_txt = query_results_to_txt | 'Change date format for txt' >> beam.ParDo(DateFormatFn())
    new_pcoll = query_results | 'Change date format' >> beam.ParDo(DateFormatFn())

    new_pcoll_to_txt | 'Write to output.txt' >> WriteToText('output.txt')

    qualified_table_name = PROJECT_ID + ':fec_modeled.Contributes_cleaned'
    # change the column "Date" in to type date
    table_schema = 'Transaction_Type:STRING, Date:DATE, Amount:STRING, Payee_ID:STRING, Contribution_ID:STRING, Year:INTEGER, Committee_Label:STRING, Candidate_Label:STRING'
    
    # write the output results as a table in BigQuery
    new_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

