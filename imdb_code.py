from bs4 import BeautifulSoup
import urllib
import re
import asyncio
import aiohttp
from collections import deque
from math import inf
from imdb_helper_functions import *
import logging

logging.basicConfig(filename='imdb.log', format='%(asctime)s %(message)s', level=logging.INFO)


async def get_actors_by_movie_soup(cast_page_soup: BeautifulSoup, num_of_actors_limit: int = None) -> list:
    """
    Getting tuples of actors and their IMDB pages and return complete list

    :parameter cast_page_soup: a soup of all cast members of a movie: BeautifulSoup
    :argument num_of_actors_limit: optional limit on how many actors to grab from a movie: int or None
    :return: cast_list (limited or complete cast members list): dictionary
    """
    # Get the table of cast
    cast = cast_page_soup.select('table.cast_list td.primary_photo a', limit=num_of_actors_limit)
    cast_list = []
    # Check if there is a limit on actors
    for actor in range(len(cast)):
        name = cast[actor].find('img')['title']
        logging.info(f"Found actor: {name}")
        actor_url = await helper_imdb_rellink(cast[actor]['href'])
        cast_list.append((name, actor_url))
    logging.info(f"Found {len(cast_list)} actors in the movie")
    return cast_list


async def get_movies_by_actor_soup(actor_page_soup: BeautifulSoup, num_of_movies_limit: int = None) -> list:
    """
    Getting the list of movies for actor with filtering to exclude non-movies and unreleased movies

    :argument actor_page_soup: BeautifulSoup
    :parameter num_of_movies_limit: optional limit on how many movies to grab for an actor: int or None
    :return: movie_list (complete or limited movies excluding tv shows and other media for given actor): list
    """
    # Find all movies where actor participated as actor
    soup = actor_page_soup.find_all('div', {'id': re.compile('act(or|ress)-[\w]+')})

    counter = 0
    movie_list = []
    # Iterate through each movie
    for title in soup:
        links = title.select('a')
        # See if previous to br element contains something like (Video Game)
        try:
            type_of_title = len(title.find('br').previous_element)
        except AttributeError:
            type_of_title = 0
        # Movies with additional status have more than one link
        if len(links) == 1 and type_of_title < 3:
            txt = links[0].get_text()
            url = await helper_imdb_rellink(links[0]['href'])
            logging.info(f"Found movie: {txt}, url: {url}")
            movie_list.append((txt, url))
            #             movie_list[txt] = url
            counter += 1
        # Check if there is a limit on number of films and abide by it
        if num_of_movies_limit and counter == num_of_movies_limit:
            logging.info(f"Limit of {num_of_movies_limit} reached!")
            break
    logging.info(f"Found {len(movie_list)} movies")
    return movie_list


async def get_movie_distance(actor_start_url: str, actor_end_url: str,
                             num_of_actors_limit: int = None,
                             num_of_movies_limit: int = None,
                             depth_limit: int = None,
                             chunk_size: int = 10) -> int or float:
    """
    Calculating the hops between actors

    :argument depth_limit: limits how deep this rabbit hole goes: int
    :parameter actor_start_url: url of actor to begin calculation: str
    :parameter actor_end_url: url of actor algorithm must traverse to: str
    :argument num_of_actors_limit: optional limit on how many actors to grab from a movie: int or None
    :argument num_of_movies_limit: optional limit on how many movies to grab for an actor: int or None
    :argument chunk_size: limit on semaphore and defines chunks for which async grab happens: int
    :return: number of hops between actors: int
    """
    actor_start_url = actor_start_url.replace('www.', '')
    actor_end_url = actor_end_url.replace('www.', '')
    forward_actors = deque()
    backward_actors = deque()
    seen_actors = set()
    seen_movies = set()
    forward_actors.append([actor_start_url])
    backward_actors.append([actor_end_url])
    distance = 0
    sem = asyncio.Semaphore(chunk_size)
    # To not overheat IMDB servers
    if not depth_limit:
        depth_limit = 3
        logging.info(f"Searching for distance between {actor_start_url} and {actor_end_url}\
                        with distance limit {depth_limit}")
    # Do bi-directional BFS traversal until either target has been found or there is intersection in the next level
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        try:
            while distance < depth_limit:
                try:
                    forward_urls = forward_actors.popleft()
                    backward_urls = backward_actors.popleft()
                except IndexError as e:
                    logging.exception(f"Deque is exhausted, no actors left to check on level {distance}")
                    raise Exception("Deque is exhausted, no actors left to check")
                # Get next level forward
                found_f, next_level_f, f_act, f_mov = await bfs(session, sem, chunk_size,
                                                                forward_urls, seen_actors, seen_movies,
                                                                num_of_actors_limit, num_of_movies_limit, actor_end_url)
                logging.info("Forward BFS successful")
                if found_f:
                    logging.info("Found in BFS forward")
                    return distance + 1
                # Checking if next level forward will include someone we would otherwise check in current level back
                if len(set(next_level_f).intersection(set(backward_urls))) > 0:
                    logging.info("Caught when checking next level of BFS-forward against current level of BFS-back")
                    distance = (2 * distance) + 1
                    return distance
                # Get next level backward
                found_b, next_level_b, b_act, b_mov = await bfs(session, sem, chunk_size,
                                                                backward_urls, seen_actors, seen_movies,
                                                                num_of_actors_limit, num_of_movies_limit,
                                                                actor_start_url)
                logging.info("Backwards BFS successful")
                if found_b:
                    logging.info("Found in BFS backward")
                    return distance + 1
                # Checking in next level backward contains someone we already checked
                if len(set(next_level_b).intersection(set(forward_urls))) > 0:
                    logging.info("Caught when checking next level of BFS-back against current level of BFS-forward")
                    distance = (2 * distance) + 1
                    return distance
                # Finally, checking if next levels will intersect so we don't have to actually grab them
                if len(set(next_level_f).intersection(set(next_level_b))) > 0:
                    logging.info("Caught when checking both next level")
                    distance = (2 * distance) + 2
                    return distance
                # If no matches were found, we advance to the next level and up the distance
                logging.info(
                    f"No matches found on this level, next to check f: {len(next_level_f)}, b: {len(next_level_b)}")
                forward_actors.append(next_level_f)
                backward_actors.append(next_level_b)
                # Updating seen lists
                seen_actors.update(f_act)
                seen_movies.update(f_mov)
                seen_actors.update(b_act)
                seen_movies.update(b_mov)
                distance += 1
        except aiohttp.ClientResponseError as e:
            logging.exception("IMDB returned non-200 status: you might have been banned.")
            raise Exception('You may have been banned').with_traceback(e.__traceback__)
    return inf


async def get_movie_descriptions_by_actor_soup(actor_page_soup: BeautifulSoup, chunk_size: int = 10) -> str:
    """
    Getting the list of movies for actor with filtering to exclude non-movies and unreleased movies

    :param chunk_size: defines the characteristics of Semaphore
    :argument actor_page_soup: BeautifulSoup
    :return: movie_list (complete or limited movies excluding tv shows and other media for given actor): list
    """
    # Find all movies where actor participated as actor
    soup = actor_page_soup.find_all('div', {'id': re.compile('act(or|ress)-[\w]+')})
    movie_list = []
    # Iterate through each movie
    for title in soup:
        links = title.select('a')
        # See if previous to br element contains something like (Video Game)
        try:
            type_of_title = len(title.find('br').previous_element)
        except AttributeError:
            type_of_title = 0
        # Movies with additional status have more than one link
        if len(links) == 1 and type_of_title < 3:
            url = urllib.parse.urljoin('https://imdb.com', links[0]['href'])
            movie_list.append(url)
    sem = asyncio.Semaphore(chunk_size)
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        try:
            movies_soup = await asyncio.gather(*[cook_soup(sem, session, url)
                                                 for url in movie_list])
            all_movies_text = await asyncio.gather(*[get_movie_description(soup)
                                                     for soup in movies_soup])
        except aiohttp.ClientResponseError as e:
            raise Exception('You may have been banned').with_traceback(e.__traceback__)
    logging.info(f"Found {len(all_movies_text)} movie descriptions")
    complete_text = " ".join(desc for desc in all_movies_text)
    return complete_text
