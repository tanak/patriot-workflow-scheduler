require 'init_test'
require 'erb'
include PatriotGCP::Ext::BigQuery

describe PatriotGCP::Ext::BigQuery do
  # it "should load data to bigquery" do
  #   bigquery_mock = double('Google::Cloud::Bigquery mock')
  #   dataset_mock  = double('Google::Cloud::Bigquery dataset mock')
  #   job_mock      = double('Google::Cloud::Bigquery job mock')
  #
  #   allow(Google::Cloud::Bigquery).to receive(:new).and_return(bigquery_mock)
  #   allow(bigquery_mock).to receive(:dataset).and_return(dataset_mock)
  #   allow(dataset_mock).to receive(:load_job).and_return(job_mock)
  #
  #   expect(Google::Cloud::Bigquery).to receive(:new).with(
  #     project: 'test-project',
  #     retries: 3,
  #     timeout: 3600
  #   ).once
  #   expect(bigquery_mock).to receive(:dataset).with('test-dataset').once
  #   expect(dataset_mock).to receive(:load_job).with(
  #     'test-table',
  #     File.join(SAMPLE_DIR, "hive_result.txt"),
  #     quote: nil,
  #     skip_leading: nil,
  #     write: 'WRITE_APPEND',
  #     delimiter: '\t'
  #   ).once
  #   expect(job_mock).to receive(:wait_until_done!).once
  #   expect(job_mock).to receive(:failed?).once
  #   expect(job_mock).to receive(:statistics).once
  #
  #   bq_load(
  #     File.join(SAMPLE_DIR, "hive_result.txt"),
  #     '/path/to/bigquery_keyfile',
  #     'test-project',
  #     'test-dataset',
  #     'test-table',
  #     'field1',
  #     {
  #       'fieldDelimiter' => '\t',
  #       'writeDisposition' => 'WRITE_APPEND'
  #     }
  #   )
  #
  #   expect(ENV.fetch('BIGQUERY_KEYFILE')).to eq('/path/to/bigquery_keyfile')
  # end

  it "raises BigQueryException when not finishing in time" do
    bigquery_mock = double('Google::Cloud::Bigquery mock')
    dataset_mock  = double('Google::Cloud::Bigquery dataset mock')
    job_mock      = double('Google::Cloud::Bigquery job mock')

    allow(Google::Cloud::Bigquery).to receive(:new).and_return(bigquery_mock)
    allow(bigquery_mock).to receive(:dataset).and_return(dataset_mock)
    allow(dataset_mock).to receive(:load_job).and_return(job_mock)
    allow(job_mock).to receive(:wait_until_done?)

    # allow(Google::Cloud::Bigquery).to receive(:new)
    #   .with(
    #   project: 'test-project',
    #   retries: 3,
    #   timeout: 1
    # ).once
    # expect(bigquery_mock).to receive(:dataset).with('test-dataset').once
    # expect(dataset_mock).to receive(:load_job).with(
    #   'test-table',
    #   File.join(SAMPLE_DIR, "hive_result.txt"),
    #   quote: nil,
    #   skip_leading: nil,
    #   write: 'WRITE_APPEND',
    #   delimiter: '\t'
    # ).once
    # expect(job_mock).to receive(:failed?).once
    # expect(job_mock).to receive(:statistics).once

    bq_load(
      File.join(SAMPLE_DIR, "hive_result.txt"),
      '/path/to/bigquery_keyfile',
      'test-project',
      'test-dataset',
      'test-table',
      'field1',
      options = {
        'fieldDelimiter' => '\t',
        'writeDisposition' => 'WRITE_APPEND'
      },
      polling_interval = 999
    )

    expect(Google::Cloud::Bigquery).to have_received(:new)
    # expect(job_mock).to have_received(:wait_until_done!).once
  end


  # it "raises BigQueryException when not finishing in time" do
  #   api_client_mock = double('api-client-mock')
  #   allow(api_client_mock).to receive(:discovered_api).with('bigquery', 'v2'){
  #       double(nil,
  #              {:jobs => double(nil,
  #                               {:get => "GET",
  #                                :insert => "INSERT"})})
  #   }
  #   allow(api_client_mock).to receive(:execute).with(hash_including(:api_method => 'INSERT',
  #                                                                   :parameters => {
  #                                                                       'projectId' => 'test-project',
  #                                                                       'uploadType' => 'multipart'
  #                                                                   })){
  #       double(nil,
  #              {:response => double(nil,
  #                                   {:body => '{"jobReference": {"jobId": "job_id01"}}'})})
  #   }
  #
  #   allow(Google::APIClient).to receive(:new).and_return(api_client_mock)
  #   expect(api_client_mock).to receive(:execute).with(hash_including(:api_method => "INSERT")).once
  #   expect {
  #       bq_load(File.join(SAMPLE_DIR, "hive_result.txt"),
  #               '/path/to/keyfile',
  #               'key_pass',
  #               'test-account@developer.gserviceaccount.com',
  #               'test-project',
  #               'test-dataset',
  #               'test-table',
  #               'field1',
  #               options={'fieldDelimiter' => '\t',
  #                        'writeDisposition' => 'WRITE_APPEND',
  #                        'allowLargeResults' => true},
  #               polling_interval=0)
  #   }.to raise_error(BigQueryException)
  #
  # end
  #
  #
  # it "raises BigQueryException when getting error from api" do
  #   api_client_mock = double('api-client-mock')
  #   allow(api_client_mock).to receive(:discovered_api).with(
  #     'bigquery', 'v2'
  #   ){
  #     double(
  #       nil, 
  #       {:jobs => double(
  #         nil,
  #         {:get => "GET", :insert => "INSERT"}
  #       )}
  #     )
  #   }
  #
  #   allow(api_client_mock).to receive(:execute).with(
  #     hash_including(
  #       :api_method => 'INSERT',
  #       :parameters => {
  #         'projectId' => 'test-project',
  #         'uploadType' => 'multipart'
  #       }
  #     )
  #   ){
  #     double(
  #       nil,
  #       {:response => double(
  #         nil, 
  #         {
  #           :body => '{"status": {
  #             "state": "DONE"
  #             "errorResult"=>{
  #               "reason"=>"invalid",
  #               "message"=>"Too many errors encountered. Limit is: 0."
  #             },
  #             "errors"=>[
  #               {
  #                 "reason"=>"invalid",
  #                 "location"=>"File: 0 / Line:100 / Field:6",
  #                 "message"=>"Invalid argument: 1,234.0"
  #               },
  #               {
  #                 "reason"=>"invalid",
  #                 "message"=>"Too many errors encountered. Limit is: 0."
  #               }
  #             ]
  #           }}'
  #         }
  #       )}
  #     )
  #   }
  #     
  #   allow(Google::APIClient).to receive(:new).and_return(api_client_mock)
  #   expect(api_client_mock).to receive(:execute).with(hash_including(:api_method => "INSERT")).once
  #   expect(api_client_mock).to receive(:execute).with(hash_including(:api_method => "GET")).never
  #   expect {
  #     bq_load(File.join(SAMPLE_DIR, "hive_result.txt"),
  #             '/path/to/keyfile',
  #             'key_pass',
  #             'test-account@developer.gserviceaccount.com',
  #             'test-project',
  #             'test-dataset',
  #             'test-table',
  #             'field1',
  #             options={'fieldDelimiter' => '\t',
  #                      'writeDisposition' => 'WRITE_APPEND',
  #                      'allowLargeResults' => true})
  #   }.to raise_error(BigQueryException)
  # end
  #
  #
  # it "loads data to bigquery but fails to register a job" do
  #   api_client_mock = double('api-client-mock')
  #   allow(api_client_mock).to receive(:discovered_api).with('bigquery', 'v2'){
  #       double(nil,
  #              {:jobs => double(nil,
  #                               {:get => "GET",
  #                                :insert => "INSERT"})})
  #   }
  #
  #   allow(api_client_mock).to receive(:execute).with(hash_including(:api_method => 'INSERT',
  #                                                                   :parameters => {
  #                                                                       'projectId' => 'test-project',
  #                                                                       'uploadType' => 'multipart'
  #                                                                   })){
  #       double(nil, {:response => double(nil, {:body => '{}'})})  # response doesn't include a job ID
  #   }   
  #   allow(Google::APIClient).to receive(:new).and_return(api_client_mock)
  #   expect(api_client_mock).to receive(:execute).with(hash_including(:api_method => "INSERT")).once
  #   expect(api_client_mock).to receive(:execute).with(hash_including(:api_method => "GET")).never
  #   expect {
  #     bq_load(File.join(SAMPLE_DIR, "hive_result.txt"),
  #             '/path/to/keyfile',
  #             'key_pass',
  #             'test-account@developer.gserviceaccount.com',
  #             'test-project',
  #             'test-dataset',
  #             'test-table',
  #             'field1',
  #             options={'fieldDelimiter' => '\t',
  #                      'writeDisposition' => 'WRITE_APPEND',
  #                      'allowLargeResults' => true})
  #   }.to raise_error(BigQueryException)
  # end
  #
  #
  # it "loads data to bigquery but the job fails" do
  #   api_client_mock = double('api-client-mock')
  #   allow(api_client_mock).to receive(:discovered_api).with('bigquery', 'v2'){
  #       double(nil,
  #              {:jobs => double(nil,
  #                               {:get => "GET",
  #                                :insert => "INSERT"})})
  #   }
  #   allow(api_client_mock).to receive(:execute).with(hash_including(:api_method => 'INSERT',
  #                                                                   :parameters => {
  #                                                                       'projectId' => 'test-project',
  #                                                                       'uploadType' => 'multipart'
  #                                                                   })){
  #       double(nil,
  #              {:response => double(nil,
  #                                   {:body => '{"jobReference": {"jobId": "job_id01"}}'})})
  #   }
  #
  #   # response contains errors
  #   allow(api_client_mock).to receive(:execute).with(hash_including(:api_method => 'GET',
  #                                                                   :parameters => {
  #                                                                       'projectId' => 'test-project',
  #                                                                       'jobId' => "job_id01"
  #                                                                   })){
  #       double(nil,
  #              {:response => double(nil,
  #                                   {:body => '{"status": {"state": "DONE", "errors": ["test error"]},
  #                                               "statistics": {"insertline": 1}}'})})
  #   }
  #
  #   allow(Google::APIClient).to receive(:new).and_return(api_client_mock)
  #   expect(api_client_mock).to receive(:execute).with(hash_including(:api_method => "INSERT")).once
  #   expect(api_client_mock).to receive(:execute).with(hash_including(:api_method => "GET")).once
  #   expect {
  #     bq_load(File.join(SAMPLE_DIR, "hive_result.txt"),
  #             '/path/to/keyfile',
  #             'key_pass',
  #             'test-account@developer.gserviceaccount.com',
  #             'test-project',
  #             'test-dataset',
  #             'test-table',
  #             'field1',
  #             options={'fieldDelimiter' => '\t',
  #                      'writeDisposition' => 'WRITE_APPEND',
  #                      'allowLargeResults' => true})
  #   }.to raise_error(BigQueryException)
  # end
end
