{{ config(schema='marts') }}

SELECT
    repo_name,
    owner,
    language,
    description,
    max(stars) as stars,
    max(forks) as forks,
    max(open_issues) as open_issues,
    max(watchers) as watchers,
    min(fetched_at) as first_seen,
    max(fetched_at) as last_updated,
    count(*) as total_snapshots
FROM staging.stg_repos
GROUP BY repo_name, owner, language, description

