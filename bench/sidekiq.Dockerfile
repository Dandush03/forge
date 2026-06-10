FROM ruby:3.3-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY Gemfile ./
RUN bundle install
COPY worker.rb probe.rb throughput.rb sidekiq-entrypoint.sh ./
RUN chmod +x sidekiq-entrypoint.sh
# Arg selects the run: `probe` (pickup latency) or `throughput`.
ENTRYPOINT ["./sidekiq-entrypoint.sh"]
