import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# If the zip code has more than five digits, just keep the first five digits
class ZipFormatFn(beam.DoFn):
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

# Project ID is needed for BigQuery data source, even for local execution.
options = {
        'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options = opts) as p:
    
    #Because of the time limits, we only extract the first 100 rows of data.
    query_results = p | 'Read from BigQuery for txt' >> beam.io.Read(beam.io.BigQuerySource(query = 'SELECT * FROM fec_modeled.Committees limit 100'))

    #write PCollection to log file
    query_results | 'Write to input.txt' >> WriteToText('input.txt')

    new_pcoll = query_results | 'Change zip code for txt' >> beam.ParDo(ZipFormatFn())

    new_pcoll | 'Write to output.txt' >> WriteToText('output.txt')

    qualified_table_name = PROJECT_ID + ':fec_modeled.Committees_Beam'
    
    # change the column ZIP into appropriate formate
    table_schema = 'ID:STRING, Name:STRING, Treasurer:STRING, Street1:STRING, Street2:STRING, City:STRING, State:STRING, ZIP:INTEGER, Designation:STRING,Type:STRING, Party_Affiliation:STRING, Filing_Frequency:STRING, Category:STRING, Year:INTEGER, Label:STRING'
    
    # write the output results as a table in BigQuery
    new_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
