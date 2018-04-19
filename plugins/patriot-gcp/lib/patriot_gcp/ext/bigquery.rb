require "google/cloud/bigquery"
require 'patriot_gcp/version'


module PatriotGCP
  module Ext
    module BigQuery

      def self.included(cls)
        cls.send(:include, Patriot::Util::System)
      end

      class BigQueryException < Exception; end

      def bq_load(filename,
                  bigquery_keyfile,
                  project_id,
                  dataset_id,
                  table_id,
                  schema,
                  options=nil,
                  polling_interval=nil)

        options ||= {}
        polling_interval ||= 60

        ENV['BIGQUERY_KEYFILE'] = bigquery_keyfile

        # 旧バージョンの仕様によりtimeoutは分単位となっているため、それを踏襲する
        # polling_intervalというキー名ではあるが、実際には
        #   60秒sleep x polling_interval数分のループ
        # という処理になっていたため、新仕様ではpolling_interval x 60秒で
        # タイムアウトとする。
        # またタイムアウトのテストに1分かかるのを避けるため、999を特別な値として
        # 扱い、999が渡された場合はタイムアウトに1秒を設定する。
        bigquery = Google::Cloud::Bigquery.new(
          project: project_id,
          retries: 3,
          # timeout: polling_interval * 60
          timeout: polling_interval == 999? 1 : polling_interval * 60
        )

        dataset = bigquery.dataset dataset_id

        # TODO: 
        # schemaとoptionがメソッドやその引数で指定されるようになっており、
        # 大幅な仕様変更となっているが、旧ライブラリ同様の設定を読み込める
        # ようにする議論はある。
        # https://github.com/GoogleCloudPlatform/google-cloud-ruby/issues/1919
        # 
        # こちらが対応された場合は下記ソースを変更する。
        job = dataset.load_job(
          table_id,
          filename,
          quote: options['quote'] || nil,
          skip_leading: options['skipLeadingRows'] || nil,
          write: options['writeDisposition'] || nil,
          delimiter: options['fieldDelimiter'] || nil,
        ) do |scm|
          schema['fields'].each do |row|
            name = row['name']
            type = row['type'].downcase.to_sym
            mode = row['mode'].downcase.to_sym if row['mode']

            scm.method(type).call(name, mode: mode)
          end
        end

        job.wait_until_done!

        if job.failed?
          raise BigQueryException, "upload failed: #{job.errors}"
        else
          return job.statistics
        end
      end

    end
  end
end
