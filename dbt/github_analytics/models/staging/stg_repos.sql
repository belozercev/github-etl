SELECT
    fetched_at,
    repo_name,
    JSONExtractString(raw_data, 'full_name') as full_name,
    JSONExtractString(raw_data, 'description') as description,
    JSONExtractString(raw_data, 'language') as language,
    JSONExtractInt(raw_data, 'stargazers_count') as stars,
    JSONExtractInt(raw_data, 'forks_count') as forks,
    JSONExtractInt(raw_data, 'open_issues_count') as open_issues,
    JSONExtractInt(raw_data, 'watchers_count') as watchers,
    JSONExtractString(raw_data, 'owner', 'login') as owner
FROM raw.github_repos
