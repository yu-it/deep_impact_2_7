#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A minimalist word-counting workflow that counts words in Shakespeare.

This is the first in a series of successively more detailed 'word count'
examples.

Next, see the wordcount pipeline, then the wordcount_debugging pipeline, for
more detailed examples that introduce additional concepts.

Concepts:

1. Reading data from text files
2. Specifying 'inline' transforms
3. Counting a PCollection
4. Writing data to Cloud Storage as text files

To execute this pipeline locally, first edit the code to specify the output
location. Output location could be a local file path or an output prefix
on GCS. (Only update the output location marked with the first CHANGE comment.)

To execute this pipeline remotely, first edit the code to set your project ID,
runner type, the staging location, the temp location, and the output location.
The specified GCS bucket(s) must already exist. (Update all the places marked
with a CHANGE comment.)

Then, run the pipeline as described in the README. It will be deployed and run
using the Google Cloud Dataflow Service. No args are required to run the
pipeline. You can see the results in your output bucket in the GCS browser.
"""

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import pvalue

Column_Definition = [
    'jockey_code',
    'register_delete_flag',
    'register_delete_date',
    'jockey_name',
    'jockey_name_kana',
    'jockey_short_name',
    'belonging_code',
    'belonging_region',
    'birthday',
    'first_licence_date',
    'apprentice_type',
    'belonging_stable',
    'jockey_comment',
    'comment_date',
    'this_year_leading',
    'this_year_performance',
    'this_year_obstacle_performance',
    'this_year_special_race_performance',
    'this_year_multi_praze',
    'last_year_leading',
    'last_year_performance',
    'last_year_obstacle_performance',
    'last_year_special_race_performance',
    'last_year_multi_praze',
    'total_performance',
    'total_obstacle_performance',
    'data_date',
    'reserve',
    'crlf'
]


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default=r'C:\\github\\deep_impact_2_7\\dataflows\\gcs_at_local\\inputs\\#local_jrdb\\a_kza.csv.gz',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      # CHANGE 1/5: The Google Cloud Storage path is required
                      # for outputting the results.
                      default='C:\github\deep_impact_2_7\dataflows\gcs_at_local\output\process_result2.txt',
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project=yu-it-base',
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=C:\github\deep_impact_2_7\dataflows\gcs_at_local\staging',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=C:\github\deep_impact_2_7\dataflows\gcs_at_local\temp',
      '--job_name=your-wordcount-job',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | ReadFromText(known_args.input)

    # Count the occurrences of each word.
#
    """
    counts = (
        lines
        | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                      .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    counts = (
        lines
        | 'Split_text_to_array' >> (beam.Map(lambda x: x.split(","))
                      .with_output_types(unicode))
        | 'Break_down_row' >> beam.FlatMap( lambda x: [[idx,x[idx]]for idx in range(len(x))])
        | 'PairWithOne' >> beam.Map(lambda x: (x[0], x[1]))
        | 'GroupBy' >> beam.GroupByKey()
        | 'Count' >> beam.combiners.Count.PerElement()

    )

    """

    class SplitOutput(beam.DoFn):
        def process(self,element):
            for idx,column in enumerate(element.split(",")):
                yield(pvalue.TaggedOutput(Column_Definition[idx], column))


    per_elements = (
    lines
    | 'Split_text_to_array' >> (beam.ParDo(SplitOutput()).with_outputs(*Column_Definition))
    )
    counts = (
        per_elements[Column_Definition[idx]]
        | 'GroupByCount_col{col}'.format(col = idx) >> beam.combiners.Count.PerElement()
        | 'CountDistinct_phase1_col{col}'.format(col = idx) >> beam.Map(lambda x : (Column_Definition[idx], 1))
        | 'GroupAndSum_col{col}'.format(col = idx) >> beam.CombinePerKey(sum)
        for idx in range(len(Column_Definition))
    ) | beam.Flatten()

    # Format the counts into a PCollection of xstrings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    #output = counts | 'Format' >> beam.Map(format_result)
    output = counts

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()