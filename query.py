from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cloud_service_account.json'
client = bigquery.Client()

def query_by_player(name:str, info: list): 
  
  # convert info from list to string to append it into the query
  info = ", " + ", ".join(info)

  query = f"""
  SELECT DISTINCT fifa_version, long_name, age, value_eur, club_name, nationality_name, player_face_url {info}
  FROM aerobic-goal-384015.soccer_player.player
  WHERE LOWER(long_name) LIKE LOWER('%{name}%') 
  ORDER BY fifa_version DESC
  """

  # run the query and return it as a dataframe
  query_job = client.query(query)
  return query_job.result().to_dataframe()

def query_by_coach(name): 
  query = f"""
  SELECT DISTINCT *
  FROM aerobic-goal-384015.soccer_player.coach
  WHERE LOWER(long_name) LIKE LOWER('%{name}%') 
  """

  # run the query and return it as a dataframe
  query_job = client.query(query)
  return query_job.result().to_dataframe()


def query_by_team(team): 
  query = f"""
  SELECT DISTINCT fifa_version, team_name, league_name, overall, coach_id
  FROM aerobic-goal-384015.soccer_player.team
  WHERE LOWER(team_name) LIKE LOWER('%{team}%')
  ORDER BY fifa_version DESC
  """

  # run the query and return it as a dataframe
  query_job = client.query(query)
  return query_job.result().to_dataframe()


def query_by_rating(rating): 
  query = f"""
  SELECT DISTINCT fifa_version, long_name, age, value_eur, club_name, nationality_name, player_face_url
  FROM aerobic-goal-384015.soccer_player.player
  WHERE overall >= {rating}
  ORDER BY fifa_version DESC
  """

  # run the query and return it as a dataframe
  query_job = client.query(query)
  return query_job.result().to_dataframe()

def query_news(name): 
  query = f"""
  SELECT DISTINCT key_word, source_name, title, description, content, url, published_at
  FROM aerobic-goal-384015.soccer_player.news
  WHERE key_word = '{name}'
  ORDER BY published_at DESC
  """

  # run the query and return it as a dataframe
  query_job = client.query(query)
  return query_job.result().to_dataframe()