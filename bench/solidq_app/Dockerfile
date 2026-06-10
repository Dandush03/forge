FROM ruby:3.3-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev libyaml-dev git \
    && rm -rf /var/lib/apt/lists/*
COPY Gemfile Gemfile.lock ./
RUN bundle install
COPY . .
RUN chmod +x solidq-entrypoint.sh bin/jobs bin/rails
ENV RAILS_ENV=development
ENTRYPOINT ["./solidq-entrypoint.sh"]
