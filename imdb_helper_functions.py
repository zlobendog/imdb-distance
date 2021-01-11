async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    """
    Asynchronous get html

    :parameter session: a ClientSession object: aiohttp.ClientSession
    :parameter url: complete url to fetch response from: str
    :return: response.text(): html code of the page: str
    """
    async with session.get(url) as response:
        return await response.text()


async def cook_soup(sem: asyncio.Semaphore, session: aiohttp.ClientSession, url: str) -> BeautifulSoup:
    """
    Asynchronous cook BeautifulSoup

    :parameter sem: a Semaphore object: asyncio.Semaphore
    :parameter session: a ClientSession object: aiohttp.ClientSession
    :parameter url: complete url of IMDB page to create a soup for: str
    :return: borsch: BeautifulSoup object of the page: BeautifulSoup
    """

    async with sem:
        ingredients = await fetch(session, url)
        borsch = BeautifulSoup(ingredients, features="lxml")
    return borsch


async def helper_imdb_rellink(url: str) -> str:
    """
    Creating a proper link from a relative link

    :parameter url: relative url from get_actors_by_movie_soup or get_movies_by_actor_soup function: str
    :return: proper_url: complete url, accessible by other functions like cook_soup: str
    """
    root_url = 'https://imdb.com/'
    proper_url = urllib.parse.urljoin(root_url, url)
    is_title = url.find('title')
    # Check if url is of movie
    if is_title > 0:
        proper_url = proper_url + 'fullcredits/'
    return proper_url


async def get_actor_name(sem: asyncio.Semaphore, session: aiohttp.ClientSession, url: str) -> str:
    """Getting the name of the actor"""
    actor_soup = await cook_soup(sem, session, url)
    try:
        name = actor_soup.select('h1 span')[0].get_text()
    except IndexError as e:
        raise Exception('You may have been banned')
    return name


async def actors_parsing(actors_list: list) -> list:
    """
    Extracting urls from list of list of tuples
    :param actors_list: a list of list of tuples: list
    :return: list of actor links: list
    """
    act_urls = []
    for l in actors_list:
        for t in l:
            act_urls.append(t[1])
    return act_urls


async def movies_parsing(movies_list: list) -> list:
    """
    Extracting movie urls from list of list of tuples
    :param movies_list: a list of list of tuples: list
    :return: list of movie links: list
    """
    mov_urls = []
    for l in movies_list:
        for t in l:
            mov_urls.append(t[1])
    return mov_urls


async def bfs(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
              chunk_size: int,
              current_urls: list,
              seen_actors: set,
              seen_movies: set,
              num_of_actors_limit: int,
              num_of_movies_limit: int,
              target_url: str):
    """
    Breadth First Search traversal algorithm
    :param session: ClientSession object
    :param sem: Semaphore object (controls concurrent connections)
    :param chunk_size: controls both Semaphore and chunking in algorithm
    :param current_urls: nodes to travers
    :param seen_actors: set of already visited actors
    :param seen_movies: set of already visited movies
    :param num_of_actors_limit: limit on how many actors to grab from a movie
    :param num_of_movies_limit: limit on how maney movies to grab for an actor
    :param target_url: target to find
    :return: found, small_actor_batch, seen_actors, seen_movies:
            target found or not, next level for BFS, sets of visited nodes
    """
    found = False
    current_urls = [x for x in current_urls if not (x in seen_actors or seen_actors.add(x))]
    movies_soup = await asyncio.gather(*[cook_soup(sem, session, actor)
                                         for actor in current_urls])
    movies = await asyncio.gather(*[get_movies_by_actor_soup(soup, num_of_movies_limit)
                                    for soup in movies_soup])
    movies = await movies_parsing(movies)
    movies = [x for x in movies if not (x in seen_movies or seen_movies.add(x))]
    small_actor_batch = []
    small_actor_batch_seen = set()
    # Chunking up the movies
    while movies:
        small_movie_batch = movies[:chunk_size]
        movies = movies[chunk_size:]
        actors_soup = await asyncio.gather(*[cook_soup(sem, session, movie) for movie in small_movie_batch])
        actors = await asyncio.gather(*[get_actors_by_movie_soup(soup, num_of_actors_limit)
                                        for soup in actors_soup])
        actors_urls = await actors_parsing(actors)
        # Checking resulted chunks
        if target_url in actors_urls:
            found = True
            small_actor_batch.extend(actors_urls)
            return found, small_actor_batch, seen_actors, seen_movies
        # Extending batch
        else:
            small_actor_batch.extend([x for x in actors_urls if not (x in small_actor_batch_seen
                                                                     or small_actor_batch_seen.add(x))])
    return found, small_actor_batch, seen_actors, seen_movies


async def get_movie_description(soup: BeautifulSoup) -> str:
    text = soup.find("div", attrs={"class": "summary_text"}).get_text().strip()
    return text


async def word_soup(url: str) -> BeautifulSoup:
    """
    Asynchronous cook BeautifulSoup

    :parameter url: complete url of IMDB page to create a soup for: str
    :return: borsch: BeautifulSoup object of the page: BeautifulSoup
    """
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        ingredients = await fetch(session, url)
        borsch = BeautifulSoup(ingredients, features="lxml")
    return borsch
