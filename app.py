from flask import Flask, render_template, request
import query
import news_etl
import cachetools

app = Flask(__name__)

# Set up a cache with a TTL of 12 hours and a maximum of 100 items
cache = cachetools.TTLCache(maxsize=100, ttl=12 * 60 * 60)

@app.route('/')
def index():
    return render_template('search.html')

@app.route('/search', methods=['POST'])
def search():
    search_query = request.form.get('search_query')
    search_by = request.form.get('search_by')
    info = request.form.getlist('info')
    
    if search_by == "player": 
        df = query.query_by_player(search_query,info)
    if search_by == "coach": 
        df = query.query_by_coach(search_query)
    if search_by == "team": 
        df = query.query_by_team(search_query)
    if search_by == "rating": 
        df = query.query_by_rating(search_query)

    table_html = df.to_html(escape=False, formatters={
        'player_face_url': lambda url: f'<img src="{url}" width="100" height="100" alt="Player Face">'}, index=False)

    # Check if the articles for the search_query are cached, otherwise fetch and cache them
    if search_query not in cache:
        articles = news_etl.extract_data(search_query)
        transformed_articles = news_etl.transform_data(articles, search_query)
        news_etl.load_data_to_bigquery(transformed_articles, "soccer_player", "news")
        cache[search_query] = transformed_articles
    
    # query the just-loaded news data from the cloud, then display them on our website
    news = query.query_news(search_query).to_html() 

    return "Search results for: {} (search by: {})".format(search_query, search_by) + table_html + "Latest news about {}".format(search_query) + news

if __name__ == '__main__':
    app.run(debug=True)
