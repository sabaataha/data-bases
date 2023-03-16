from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor


# ADD CHECK &
# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "DROP TABLE IF EXISTS Critic CASCADE;CREATE TABLE Critic( \n" +
            "CriticId INTEGER PRIMARY KEY, \n " +
            "CriticName text NOT NULL, "
            "CHECK(CriticId>0));  \n " +
            "DROP TABLE IF EXISTS Movie CASCADE; CREATE TABLE Movie("
            "MovieName text NOT NULL, \n " +
            "MovieYear INTEGER NOT NULL,\n" +
            "MovieGenre text NOT NULL,\n" +
            "CHECK ( MovieGenre='Drama' or MovieGenre='Comedy' or MovieGenre='Horror' or MovieGenre='Action' ),"
            "CHECK (MovieYear>=1895), \n " +
            "PRIMARY KEY(MovieYear,MovieName));\n " +
            "DROP TABLE IF EXISTS Actor CASCADE;CREATE TABLE Actor( \n " +
            "ActorID INTEGER NOT NULL, \n " +
            "ActorName text NOT NULL, \n " +
            "ActorAge integer NOT NULL, \n " +
            "ActorHeight INTEGER NOT NULL, \n" +
            "PRIMARY KEY(ActorID) ,CHECK(ActorAge>0),CHECK(ActorID>0),CHECK(ActorHeight>0)); \n" +
            "DROP TABLE IF EXISTS Studio CASCADE;CREATE TABLE Studio( \n " +
            "StudioId INTEGER PRIMARY KEY, "
            "StudioName text NOT NULL, "
            "CHECK(StudioId>0)); \n" +
            "DROP TABLE IF EXISTS CriticRatedMovie CASCADE; CREATE TABLE CriticRatedMovie( \n " +
            "CriticId INTEGER NOT NULL, \n" +
            "MovieName text NOT NULL, \n " +
            "MovieYear INTEGER NOT NULL, \n " +
            "Rating INTEGER NOT NULL,\n" +
            "FOREIGN KEY (CriticId) REFERENCES Critic(CriticId) ON DELETE CASCADE, \n " +
            "FOREIGN KEY (MovieName,MovieYear) REFERENCES Movie(MovieName,MovieYear) ON DELETE CASCADE, \n"
            "PRIMARY KEY(MovieName,MovieYear,CriticId)," +
            "CHECK(Rating>=1 and Rating<=5)); \n" +
            "DROP TABLE IF EXISTS StudioProduceMovie CASCADE;CREATE TABLE StudioProduceMovie( \n" +
            "StudioId INTEGER NOT NULL, \n" +
            "MovieName text NOT NULL, \n" +
            "MovieYear INTEGER NOT NULL, \n" +
            "Budget INTEGER NOT NULL, \n" +
            "Revenue INTEGER NOT NULL,\n" +
            "FOREIGN KEY (StudioId) REFERENCES Studio(StudioId) ON DELETE CASCADE, \n" +
            "FOREIGN KEY (MovieName,MovieYear) REFERENCES Movie(MovieName,MovieYear) ON DELETE CASCADE, "
            "PRIMARY KEY(StudioId,MovieName,MovieYear),"
            "CHECK (Budget>=0),CHECK (Revenue>=0), " +
            " UNIQUE(MovieName,MovieYear));\n " +
            "DROP TABLE IF EXISTS ActorPlayedMovie CASCADE;CREATE TABLE ActorPlayedMovie( \n " +
            "ActorID INTEGER NOT NULL, \n" +
            "MovieName text NOT NULL, \n" +
            "MovieYear INTEGER NOT NULL, \n" +
            "ActorSalary INTEGER NOT NULL, \n" +
            "ActorRole INTEGER NOT NULL, \n"
            "CHECK(ActorSalary>0),CHECK(ActorRole>0)," +
            "FOREIGN KEY (MovieName,MovieYear) REFERENCES Movie(MovieName,MovieYear) ON DELETE CASCADE, \n" +
            "FOREIGN KEY (ActorID) REFERENCES Actor(ActorID) ON DELETE CASCADE,"
            "PRIMARY KEY(ActorID,MovieName,MovieYear) ); \n" +
            "DROP VIEW IF EXISTS ActorMovieRating CASCADE;CREATE VIEW ActorMovieRating AS\n" +
            "SELECT COALESCE(CM.Rating,0) as Rating, A.ActorID as ActorID, A.MovieName as MovieName, A.MovieYear as \n" +
            "MovieYear \n" +
            "FROM ActorPlayedMovie A LEFT OUTER JOIN CriticRatedMovie CM ON (A.MovieName = CM.MovieName AND " +
            "A.MovieYear = CM.MovieYear);\n" +
            "DROP TABLE IF EXISTS ActorRolesInMovies CASCADE;CREATE TABLE ActorRolesInMovies( \n" +
            "ActorID INTEGER NOT NULL, \n"
            "ActorRoles TEXT NOT NULL, "
            "MovieName text NOT NULL, \n" +
            "MovieYear INTEGER NOT NULL, \n" +
            "FOREIGN KEY (ActorID) REFERENCES Actor(ActorID) ON DELETE CASCADE, \n"
            "FOREIGN KEY (MovieName,MovieYear) REFERENCES Movie(MovieName,MovieYear) ON DELETE CASCADE, "
            "FOREIGN KEY (MovieName,MovieYear,ActorID) REFERENCES ActorPlayedMovie(MovieName,MovieYear,ActorID) ON DELETE CASCADE); "
            "COMMIT;"
        )
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("BEGIN; \n" +
                         "DELETE FROM Actor; \n" +
                         "DELETE FROM Movie; \n" +
                         "DELETE FROM Studio; \n" +
                         "DELETE FROM Critic; \n" +
                         "DELETE FROM CriticRatedMovie; \n" +
                         "DELETE FROM StudioProduceMovie; \n" +
                         "DELETE FROM ActorPlayedMovie; \n" +
                         "DELETE FROM ActorRolesInMovies; \n" +
                         "COMMIT;")
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN; \n" +
                     "DROP TABLE IF EXISTS Actor,Movie,Studio,Critic,ActorPlayedMovie, StudioProduceMovie,"
                     " CriticRatedMovie,ActorRolesInMovies CASCADE; \n" +
                     "DROP VIEW IF EXISTS ActorMovieRating CASCADE; \n" +
                     "COMMIT; ")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    er = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Critic(CriticId, CriticName) VALUES({Cid}, {Cname})") \
            .format(
            Cid=sql.Literal(critic.getCriticID()),
            Cname=sql.Literal(critic.getName()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        print(e)
        er = 6
    finally:
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def deleteCritic(critic_id: int) -> ReturnValue:
    er = 0
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("DELETE FROM Critic WHERE CriticId={0}").format(sql.Literal(critic_id))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        print(e)
        er = 6
    finally:
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def getCriticProfile(critic_id: int) -> Critic:
    ret_critic = Critic()
    flag = False
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("SELECT * FROM Critic WHERE CriticId={cid}").format(cid=sql.Literal(critic_id))
        rows_effected, result = conn.execute(query1)
        conn.commit()
        ret_critic.setCriticID((result.rows.__getitem__(0))[0])
        ret_critic.setName((result.rows.__getitem__(0))[1])
    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or rows_effected != 1:
            conn.close()
            return Critic.badCritic()
        conn.close()
        return ret_critic


def addActor(actor: Actor) -> ReturnValue:
    er = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("INSERT INTO Actor(ActorID, ActorName, ActorAge, ActorHeight) VALUES({aid},"
                         " {aname}, {aage},{ahieght})").format(
            aid=sql.Literal(actor.getActorID()),
            aname=sql.Literal(actor.getActorName()),
            aage=sql.Literal(actor.getAge()),
            ahieght=sql.Literal(actor.getHeight()))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        conn.close()
        return ReturnValue.OK


def deleteActor(actor_id: int) -> ReturnValue:
    er = 0
    rows_effected = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("DELETE FROM Actor WHERE ActorID={0}").format(sql.Literal(actor_id))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def getActorProfile(actor_id: int) -> Actor:
    ret_actor = Actor()
    flag = False
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("SELECT * FROM Actor WHERE ActorID={0}").format(sql.Literal(actor_id))
        rows_effected, result = conn.execute(query1)
        conn.commit()
        ret_actor.setActorID((result.rows.__getitem__(0))[0])
        ret_actor.setActorName((result.rows.__getitem__(0))[1])
        ret_actor.setAge((result.rows.__getitem__(0))[2])
        ret_actor.setHeight((result.rows.__getitem__(0))[3])
    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or rows_effected != 1:
            conn.close()
            return Actor.badActor()
        conn.close()
        return ret_actor


def addMovie(movie: Movie) -> ReturnValue:
    er = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "INSERT INTO Movie(MovieName, MovieYear, MovieGenre) VALUES({mname}, {myear}, {mgenre})") \
            .format(
            mname=sql.Literal(movie.getMovieName()),
            myear=sql.Literal(movie.getYear()),
            mgenre=sql.Literal(movie.getGenre()))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        print(e)
        er = 6
    finally:
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    er = 0
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "DELETE FROM Movie WHERE MovieName={mname} and MovieYear={myear}"). \
            format(mname=sql.Literal(movie_name),
                   myear=sql.Literal(year))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def getMovieProfile(movie_name: str, year: int) -> Movie:
    ret_movie = Movie()
    flag = False
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "SELECT * FROM Movie WHERE MovieName={mname} AND MovieYear={myear}"). \
            format(mname=sql.Literal(movie_name)
                   , myear=sql.Literal(year))
        rows_effected, result = conn.execute(query1)
        conn.commit()
        ret_movie.setMovieName((result.rows.__getitem__(0))[0])
        ret_movie.setYear((result.rows.__getitem__(0))[1])
        ret_movie.setGenre((result.rows.__getitem__(0))[2])
    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or rows_effected != 1:
            conn.close()
            return Movie.badMovie()
        conn.close()
        return ret_movie


def addStudio(studio: Studio) -> ReturnValue:
    conn = None
    er = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Studio(StudioId, StudioName) VALUES({Sid}, {Sname})") \
            .format(
            Sid=sql.Literal(studio.getStudioID()),
            Sname=sql.Literal(studio.getStudioName()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


# add cascade
def deleteStudio(studio_id: int) -> ReturnValue:
    er = 0
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("DELETE FROM Studio WHERE StudioId={0}").format(sql.Literal(studio_id))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def getStudioProfile(studio_id: int) -> Studio:
    ret_studio = Studio()
    flag = False
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("SELECT * FROM Studio WHERE StudioId={sId}").format(sId=sql.Literal(studio_id))
        rows_effected, result = conn.execute(query1)
        conn.commit()
        ret_studio.setStudioID((result.rows.__getitem__(0))[0])
        ret_studio.setStudioName((result.rows.__getitem__(0))[1])

    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or rows_effected != 1:
            conn.close()
            return Studio.badStudio()
        conn.close()
        return ret_studio


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    er = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "INSERT INTO CriticRatedMovie(CriticId,MovieName, MovieYear, Rating) VALUES({cid},{mname}, {myear} \n" +
            ", {rating})").format(
            cid=sql.Literal(criticID),
            mname=sql.Literal(movieName),
            myear=sql.Literal(movieYear),
            rating=sql.Literal(rating))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except Exception as e:
        er = 6
    finally:
        if er == 2 or er == 3:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 5:
            conn.close()
            return ReturnValue.NOT_EXISTS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        conn.close()
        return ReturnValue.OK


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    er = 0
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "DELETE FROM CriticRatedMovie WHERE MovieName={mname} and MovieYear={myear} and CriticId={cid}") \
            .format(
            cid=sql.Literal(criticID),
            mname=sql.Literal(movieName),
            myear=sql.Literal(movieYear))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        er = 1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 2
    except DatabaseException.CHECK_VIOLATION as e:
        er = 3
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if er == 1 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 2 or er == 3 or er == 5 or rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    er = 0
    conn = None
    i = len(roles)
    try:
        conn = Connector.DBConnector()
        query1 = 'INSERT INTO ActorRolesInMovies(MovieName, MovieYear,ActorID, ActorRoles) VALUES '
        i = 0
        while i < (len(roles)):
            if i < (len(roles) - 1):
                query1 += '({mname}, {myear}, {aid}, {role}),'.format(
                    aid=actorID,
                    mname=(sql.Literal(movieName)).as_string(conn.cursor),
                    myear=movieYear,
                    role=(sql.Literal(roles[i])).as_string(conn.cursor))
            else:
                query1 += '({mname}, {myear}, {aid}, {role})'.format(
                    aid=actorID,
                    mname=(sql.Literal(movieName)).as_string(conn.cursor),
                    myear=movieYear,
                    role=(sql.Literal(roles[i])).as_string(conn.cursor))
            i = i + 1
        # rows_effected, _ = conn.execute(query1)
        # conn.commit()
        # if i < (len(roles)) or (i==0):
        #     conn.close()
        #     return ReturnValue.BAD_PARAMS
        query2 = "; INSERT INTO ActorPlayedMovie(ActorID,MovieName, MovieYear, ActorSalary, ActorRole) VALUES "
        query2 += "({aid},{mname},{myear},{salary},{roles});".format(values=sql.Literal(query1),
                                                                     aid=actorID,
                                                                     mname=(sql.Literal(movieName)).as_string(
                                                                         conn.cursor),
                                                                     myear=movieYear,
                                                                     salary=salary,
                                                                     roles=len(roles))

        # final_query2 = sql.SQL("INSERT INTO ActorPlayedMovie(ActorID,MovieName, MovieYear, ActorSalary, ActorRole) "
        #         "VALUES ({aid},{mname},{myear},{salary},{roles});") \
        #         .format(
        #         aid=sql.Literal(actorID),
        #         mname=sql.Literal(movieName),
        #         myear=sql.Literal(movieYear),
        #         salary=sql.Literal(salary),
        #         roles=sql.Literal(len(roles)))
        final_query1 = query2 + query1
        rows_effected, _ = conn.execute(final_query1)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 1
    except DatabaseException.CHECK_VIOLATION as e:
        er = 2
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.ConnectionInvalid as e:
        er = 5
    except Exception as e:
        # print(e)
        er = 6
    finally:
        if i == 0:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 5 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 1 or er == 2:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 3:
            conn.close()
            return ReturnValue.NOT_EXISTS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def getActorsRoleInMovie(actor_id : int, movie_name : str, movieYear :int) -> List[str]:
    flag = False
    conn = None
    rows_effected = 0
    ret_list=[]
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL("SELECT ActorRoles FROM ActorRolesInMovies WHERE ActorID={aid} AND MovieName={mname} "
                         "AND MovieYear={myear} "
                         "ORDER BY ActorRoles DESC").format(aid=sql.Literal(actor_id),
                                                         mname=sql.Literal(movie_name),
                                                         myear=sql.Literal(movieYear))
        rows_effected, result = conn.execute(query1)
        conn.commit()
        for i in range(0,rows_effected):
            tup=((result.rows.__getitem__(i))[0])
            ret_list.append(tup)

    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        print(e)
        flag = True
    finally:
        if flag or rows_effected == 0:
            conn.close()
            return []
        conn.close()
        return ret_list


def actorDidntPlayInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    er = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "DELETE FROM ActorPlayedMovie WHERE MovieName={mname} and MovieYear={myear} and ActorID={aid}") \
            .format(
            aid=sql.Literal(actorID),
            mname=sql.Literal(movieName),
            myear=sql.Literal(movieYear))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 3
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 1
    except DatabaseException.CHECK_VIOLATION as e:
        er = 2
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.ConnectionInvalid as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if er == 5 or er == 6 or er == 1 or er == 2:
            conn.close()
            return ReturnValue.ERROR
        if er == 3 or rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    er = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "INSERT INTO StudioProduceMovie(StudioId,MovieName, MovieYear, Budget,Revenue) VALUES({sid},{mname}, {myear}, "
            "{budget},{revenue})").format(
            sid=sql.Literal(studioID),
            mname=sql.Literal(movieName),
            myear=sql.Literal(movieYear),
            budget=sql.Literal(budget),
            revenue=sql.Literal(revenue))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 1
    except DatabaseException.CHECK_VIOLATION as e:
        er = 2
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.ConnectionInvalid as e:
        er = 5
    except Exception as e:
        print(e)
        er = 6
    finally:
        if er == 5 or er == 6:
            conn.close()
            return ReturnValue.ERROR
        if er == 1 or er == 2:
            conn.close()
            return ReturnValue.BAD_PARAMS
        if er == 3:
            conn.close()
            return ReturnValue.NOT_EXISTS
        if er == 4:
            conn.close()
            return ReturnValue.ALREADY_EXISTS
        conn.close()
        return ReturnValue.OK


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    er = 0
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "DELETE FROM StudioProduceMovie WHERE StudioId={sid} and MovieName={mname} and MovieYear={myear} ") \
            .format(
            sid=sql.Literal(studioID),
            mname=sql.Literal(movieName),
            myear=sql.Literal(movieYear))
        rows_effected, _ = conn.execute(query1)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        er = 1
    except DatabaseException.CHECK_VIOLATION as e:
        er = 2
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        er = 3
    except DatabaseException.UNIQUE_VIOLATION as e:
        er = 4
    except DatabaseException.ConnectionInvalid as e:
        er = 5
    except Exception as e:
        er = 6
    finally:
        if er > 0:
            conn.close()
            return ReturnValue.ERROR
        if rows_effected == 0:
            conn.close()
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    flag = False
    conn = None
    avg = 0
    try:
        conn = Connector.DBConnector()
        query1 = sql.SQL(
            "SELECT AVG(Rating) FROM CriticRatedMovie WHERE MovieName={mname} AND MovieYear={myear}") \
            .format(
            mname=sql.Literal(movieName),
            myear=sql.Literal(movieYear))
        rows_effected, result = conn.execute(query1)
        conn.commit()
        if rows_effected == 1:
            avg = ((result.rows.__getitem__(0))[0])
        if avg == None:
            avg = 0
    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or result is 0:
            conn.close()
            return 0
        conn.close()
        return avg


def averageActorRating(actorID: int) -> float:
    flag = False
    conn = None
    avg = 0
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query0 = sql.SQL("DROP VIEW IF EXISTS avgRating CASCADE;CREATE VIEW avgRating AS "
                         "SELECT AVG(Rating) as avgRating "
                         "FROM ActorMovieRating "
                         "WHERE ActorID={aid} "
                         "GROUP BY (MovieName, MovieYear)").format(
            aid=sql.Literal(actorID))
        rows_effected, result = conn.execute(query0)
        conn.commit()
        query1 = sql.SQL(
            "SELECT (SUM(avgRating)/count(*)) as allAvg "
            "FROM avgRating") \
            .format(
            aid=sql.Literal(actorID))
        rows_effected, result = conn.execute(query1)
        conn.commit()
        if rows_effected == 1:
            avg = ((result.rows.__getitem__(0))[0])
    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or rows_effected == 0 or avg == None:
            conn.close()
            return 0
        conn.close()
        return float(avg)


def bestPerformance(actor_id: int) -> Movie:
    flag = False
    ret_movie = Movie()
    conn = None
    result = 0
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        # query1 = sql.SQL("DROP VIEW IF EXISTS Trial CASCADE;CREATE VIEW Trial AS\n" +
        #     "SELECT AVG(A.Rating) as AvgRating , M.MovieGenre as MovieGenre,"
        #     " M.MovieName as MovieName, M.MovieYear as MovieYear \n" +
        #     "FROM ActorMovieRating A JOIN Movie M ON (A.MovieName = M.MovieName AND " +
        #     "A.MovieYear = M.MovieYear) "
        #     "WHERE A.ActorID={aid} "
        #     "GROUP BY (M.MovieName,M.MovieYear,M.MovieGenre)").format(
        #     aid=sql.Literal(actor_id))
        # rows_effected, result = conn.execute(query1)
        # conn.commit()
        query = sql.SQL(
            "SELECT AMC.MovieName, AMC.MovieYear, AMC.MovieGenre "
            "FROM (SELECT AVG(A.Rating) as AvgRating , M.MovieGenre as MovieGenre,"
            " M.MovieName as MovieName, M.MovieYear as MovieYear \n" +
            "FROM ActorMovieRating A JOIN Movie M ON (A.MovieName = M.MovieName AND " +
            "A.MovieYear = M.MovieYear) "
            "WHERE A.ActorID={aid} "
            "GROUP BY (M.MovieName,M.MovieYear,M.MovieGenre)) AS AMC "
            "ORDER BY AMC.AvgRating DESC , AMC.MovieYear ASC, AMC.MovieName DESC "
            "LIMIT 1 ").format(
            aid=sql.Literal(actor_id))
        rows_effected, result = conn.execute(query)
        conn.commit()
        ret_movie.setMovieName((result.rows.__getitem__(0))[0])
        ret_movie.setYear((result.rows.__getitem__(0))[1])
        ret_movie.setGenre((result.rows.__getitem__(0))[2])

    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or rows_effected is 0:
            conn.close()
            return Movie.badMovie()
        conn.close()
        return ret_movie


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    conn = None
    flag = False
    rows_effected1 = 0
    budget = 0
    sum_salary = 0
    try:
        conn = Connector.DBConnector()
        rows_effected0, result0 = conn.execute("DROP VIEW IF EXISTS BudgetSalary CASCADE;\n" +
                                               "CREATE VIEW BudgetSalary AS\n" +
                                               "SELECT COALESCE(sm.Budget,0) as Budget, "
                                               "COALESCE(sm.MovieName,am.MovieName) as MovieName, "
                                               "COALESCE(sm.MovieYear,am.MovieYear) as MovieYear, "
                                               "COALESCE(am.ActorSalary,0) as Salary \n"
                                               "FROM StudioProduceMovie as sm \n"
                                               "FULL OUTER JOIN  ActorPlayedMovie as am \n"
                                               " ON sm.movieName=am.movieName and sm.movieYear=am.movieYear;")
        conn.commit()
        query1 = sql.SQL("SELECT (MAX(COALESCE(BS.Budget,0)) - SUM(COALESCE(BS.Salary,0))) AS result "
                         "FROM BudgetSalary as BS RIGHT OUTER JOIN Movie as M "
                         "ON(BS.MovieName=M.MovieName and BS.MovieYear=M.MovieYear) "
                         "WHERE M.MovieName={movieName} AND M.MovieYear={movieYear} "
                         "GROUP BY M.MovieName, M.MovieYear ").format(
            movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear))
        rows_effected1, result1 = conn.execute(query1)
        conn.commit()
        budget = ((result1.rows.__getitem__(0))[0])
        # sum_salary = ((result.rows.__getitem__(0))[1])
        rows_effected2, result2 = conn.execute("DROP VIEW BudgetSalary")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        flag = True
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = True
    except DatabaseException.CHECK_VIOLATION as e:
        flag = True
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = True
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = True
    except Exception as e:
        flag = True
    finally:
        if flag or rows_effected1 == 0:
            conn.close()
            return -1
        conn.close()
        return budget


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    flag = True
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * "
            "FROM ActorPlayedMovie "
            "WHERE MovieName={mname} and MovieYear={myear} "
            "AND ActorID={aid} "
            "AND ActorRole>=COALESCE((SELECT SUM(ActorRole) "
            "FROM ActorPlayedMovie "
            "WHERE MovieName={mname} and MovieYear={myear} and ActorID<>{aid}),0) ") \
            .format(
            mname=sql.Literal(movie_name),
            myear=sql.Literal(movie_year),
            aid=sql.Literal(actor_id))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        flag = False
    except DatabaseException.NOT_NULL_VIOLATION as e:
        flag = False
    except DatabaseException.CHECK_VIOLATION as e:
        flag = False
    except DatabaseException.UNIQUE_VIOLATION as e:
        flag = False
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        flag = False
    except Exception as e:
        print(e)
        flag = False
    finally:
        if flag is False or rows_effected == 0:
            conn.close()
            return False
        conn.close()
        return True


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    result = 0
    ret_list = []
    try:
        conn = Connector.DBConnector()
        # first, create view for Movie and StudioProducedMovie with left outer Join

        rows_effected, result = conn.execute("DROP VIEW IF EXISTS MovieRevenue CASCADE;\n" +
                                             "CREATE VIEW MovieRevenue AS\n" +
                                             "SELECT M.MovieName as MovieName, M.MovieYear as MovieYear, SM.Revenue as Revenue \n" +
                                             "FROM Movie M LEFT OUTER JOIN StudioProduceMovie SM ON (M.MovieName = SM.MovieName AND " +
                                             "M.MovieYear = SM.MovieYear);\n")
        # now we have a view that holds movie name movie id and revenue
        conn.commit()
        rows_effected, result = conn.execute("SELECT MovieName, COALESCE(SUM(Revenue),0) \n"
                                             "FROM MovieRevenue \n"
                                             "GROUP BY MovieName \n"
                                             "ORDER BY MovieName DESC;")
        conn.commit()
        # insert tuples
        i = 0
        while i < rows_effected:
            tuple_to_insert = ((result.rows.__getitem__(i))[0], (result.rows.__getitem__(i))[1])
            ret_list.append(tuple_to_insert)
            i = i + 1
        rows_effected, result = conn.execute("DROP VIEW MovieRevenue")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        pass
    except DatabaseException.NOT_NULL_VIOLATION as e:
        pass
    except DatabaseException.CHECK_VIOLATION as e:
        pass
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        pass
    except Exception as e:
        pass
    finally:
        conn.close()
        return ret_list


def studioRevenueByYear() -> List[Tuple[str, int]]:
    conn = None
    result = 0
    ret_list = []
    try:
        conn = Connector.DBConnector()

        rows_effected, result = conn.execute("SELECT StudioId,MovieYear, SUM(Revenue) \n"
                                             "FROM StudioProduceMovie \n"
                                             "GROUP BY (StudioId,MovieYear) \n"
                                             "ORDER BY (StudioId,MovieYear) DESC ")
        # insert tuples
        i = 0
        while i < rows_effected:
            tuple_to_insert = ((result.rows.__getitem__(i))[0],
                               (result.rows.__getitem__(i))[1], (result.rows.__getitem__(i))[2])
            ret_list.append(tuple_to_insert)
            i = i + 1
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        pass
    except DatabaseException.NOT_NULL_VIOLATION as e:
        pass
    except DatabaseException.CHECK_VIOLATION as e:
        pass
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        pass
    except Exception as e:
        print(e)
        pass
    finally:
        conn.close()
        return ret_list


def getFanCritics() -> List[Tuple[int, int]]:
    conn = None
    result = 0
    ret_list = []
    try:
        conn = Connector.DBConnector()
        # first, create view for CriticRatedMove and StudioProducedMovie with Join

        rows_effected0, result0 = conn.execute("DROP VIEW IF EXISTS CriticFan CASCADE;\n" +
                                               "CREATE VIEW CriticFan AS \n" +
                                               "SELECT SM.StudioId as StudioId, CM.CriticId as CriticId, " +
                                               "COUNT(*) as RatingCount " +
                                               "FROM StudioProduceMovie SM LEFT OUTER JOIN CriticRatedMovie CM ON " +
                                               "(CM.MovieName = SM.MovieName AND CM.MovieYear = SM.MovieYear) " +
                                               "GROUP BY SM.StudioId, CM.CriticId;\n")
        conn.commit()

        rows_effected1, result1 = conn.execute("SELECT CriticId, StudioId "
                                               "FROM CriticFan CF "
                                               "WHERE CriticId IS NOT NULL AND "
                                               "RatingCount=(SELECT COUNT(*) FROM StudioProduceMovie "
                                               "WHERE StudioId = CF.StudioId "
                                               "GROUP BY StudioId) "
                                               "ORDER BY CriticId DESC,StudioId DESC")
        conn.commit()
        # insert tuples
        i = 0
        while i < rows_effected1:
            tuple_to_insert = ((result1.rows.__getitem__(i))[0], (result1.rows.__getitem__(i))[1])
            ret_list.append(tuple_to_insert)
            i = i + 1
        rows_effected2, result2 = conn.execute("DROP VIEW CriticFan")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        pass
    except DatabaseException.NOT_NULL_VIOLATION as e:
        pass
    except DatabaseException.CHECK_VIOLATION as e:
        pass
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        pass
    except Exception as e:
        print(e)
        pass
    finally:
        conn.close()
        return ret_list


def averageAgeByGenre() -> List[Tuple[str, float]]:
    conn = None
    ret_list = []
    try:
        conn = Connector.DBConnector()
        rows_effected0, result0 = conn.execute("DROP VIEW IF EXISTS AverageGenre CASCADE;\n" +
                                               "CREATE VIEW AverageGenre AS\n" +
                                               "SELECT M.MovieGenre AS MovieGenre, AM.ActorID AS ActorId \n"
                                               "FROM Movie M INNER JOIN ActorPlayedMovie AM ON (M.MovieName = AM.MovieName AND " +
                                               "M.MovieYear = AM.MovieYear )"
                                               "GROUP BY (MovieGenre,ActorID);\n")
        conn.commit()
        rows_effected1, result1 = conn.execute("SELECT AG.MovieGenre AS MovieGenre, AVG(A.ActorAge) \n"
                                               "FROM AverageGenre AG INNER JOIN Actor A ON (AG.ActorId = A.ActorID ) \n"
                                               "GROUP BY MovieGenre "
                                               "ORDER BY MovieGenre ASC;")
        conn.commit()
        # insert tuples
        i = 0
        while i < rows_effected1:
            tuple_to_insert = (((result1.rows.__getitem__(i))[0]), float((result1.rows.__getitem__(i))[1]))
            ret_list.append(tuple_to_insert)
            i = i + 1
        rows_effected2, result2 = conn.execute("DROP VIEW AverageGenre")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        pass
    except DatabaseException.NOT_NULL_VIOLATION as e:
        pass
    except DatabaseException.CHECK_VIOLATION as e:
        pass
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        pass
    except Exception as e:
        print(e)
        pass
    finally:
        conn.close()
        return ret_list


def getExclusiveActors() -> List[Tuple[int, int]]:
    conn = None
    ret_list = []
    try:
        conn = Connector.DBConnector()
        # first, create view for CriticRatedMove and StudioProducedMovie with Join

        rows_effected0, result0 = conn.execute("DROP VIEW IF EXISTS ActorStudio CASCADE;\n" +
                                               "CREATE VIEW ActorStudio AS\n" +
                                               "SELECT SM.StudioId AS StudioId, AM.ActorID AS ActorID \n"
                                               "FROM StudioProduceMovie SM INNER JOIN ActorPlayedMovie AM ON (AM.MovieName = SM.MovieName AND " +
                                               "AM.MovieYear = SM.MovieYear)"
                                               "GROUP BY (ActorID,StudioId);\n")
        conn.commit()
        rows_effected1, result1 = conn.execute("SELECT ActorID, StudioId\n"
                                               "FROM ActorStudio \n"
                                               "WHERE ActorID IS NOT NULL AND StudioId IS NOT NULL "
                                               "AND ActorId IN"
                                               "(SELECT ActorID FROM ActorStudio \n"
                                               "GROUP BY ActorID\n"
                                               " HAVING COUNT(*)=1) \n"
                                               "ORDER BY ActorID DESC ;")
        conn.commit()
        # insert tuples
        i = 0
        while i < rows_effected1:
            tuple_to_insert = ((result1.rows.__getitem__(i))[0], (result1.rows.__getitem__(i))[1])
            ret_list.append(tuple_to_insert)
            i = i + 1
        rows_effected2, result2 = conn.execute("DROP VIEW ActorStudio")
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        pass
    except DatabaseException.NOT_NULL_VIOLATION as e:
        pass
    except DatabaseException.CHECK_VIOLATION as e:
        pass
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        pass
    except Exception as e:
        print(e)
        pass
    finally:
        conn.close()
        return ret_list
