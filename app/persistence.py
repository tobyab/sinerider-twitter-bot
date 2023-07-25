from pyairtable import Table
from pyairtable.formulas import EQUAL, AND, IF, FIELD, to_airtable_value


class Persistence:
    def __init__(self, airtable_api_key, airtable_base_id):
        """ Constructor
        :param airtable_api_key: API key for airtable
        :param airtable_base_id: Base ID for airtable
        """
        self.work_queue_table = Table(airtable_api_key, airtable_base_id, "TwitterWorkQueue")
        self.leaderboard_table = Table(airtable_api_key, airtable_base_id, "Leaderboard")
        self.config_table = Table(airtable_api_key, airtable_base_id, "Config")
        self.puzzle_table = Table(airtable_api_key, airtable_base_id, "Puzzles")

    def config_exists(self, key):
        """ Checks whether a config exists with a given key
        :param key: The key whose existence you'd like to check
        :return: True if exists, False if not
        """
        return self.get_one_row(self.config_table, key, None) is not None

    def get_config(self, key, default):
        """ Gets a config associated with the key
        :param key: The key whose value you want
        :param default: The default value to return if the key didn't exist
        :return: The value associated with the key or the default value
        """
        val = self.get_one_row(self.config_table, "config_name", key)
        if val is None:
            return default
        return val["fields"]["value"]

    def queue_work(self, tweetId, twitterHandle, puzzleId, expression):
        """ Queues a user-submitted solution to the work queue for later processing
        :param tweetId: The ID of the tweet that was submitted
        :param twitterHandle: The twitter handle of the user that tweeted the submission
        :param puzzleId: The ID of the puzzle associated with the submission
        :param expression: The sinerider graph expression in the submission
        """
        print("queueing: %s %s %s %s" % (tweetId, twitterHandle, puzzleId, expression))
        self.work_queue_table.create(
            {"tweetId": tweetId, "twitterHandle": twitterHandle, "puzzleId": puzzleId, "expression": expression,
             "completed": False, "attempts": 0})

    def get_all_queued_work(self):
        """ Returns all queued non-completed work in the work queue (submissions to be scored and responded to)
        :return: A list of rows from the work queue table
        """
        formula = AND(EQUAL(FIELD("completed"), to_airtable_value(0)), IF("{attempts} < 3", 1, 0))
        return self.work_queue_table.all(formula=formula)

    def get_one_row(self, table, fieldToMatch, value):
        """ Returns one row from a table with an optional field to match, or None
        :param table: The table you'd like to get a row from
        :param fieldToMatch: The field that you would like to match
        :param value: The value you'd like to look up in 'fieldToMatch'
        :return: One row from the table, or None
        """
        formula = EQUAL(FIELD(fieldToMatch), to_airtable_value(value))
        try:
            all_rows = table.all(formula=formula)
            if len(all_rows) == 0:
                return None

            return all_rows[0]
        except:
            print("Shouldn't have happened!")
        return None

    def validate_puzzle_id(self, puzzle_id):
        """ Returns whether the given puzzle_id is valid
        :param puzzle_id: A puzzle id that you'd like to validate as existent and actionable
        :return: True if the puzzle exists and can be interacted with, otherwise False
        """
        # Note - we need to ensure the puzzle exists...
        try:
            puzzle = self.get_one_row(self.puzzle_table, "id", puzzle_id)
            if puzzle is not None:
                return True
        except:
            print("error validating puzzle id, probably failed to query airtable")
        return False

    def complete_queued_work(self, tweet_id):
        """ Marks a particular piece of work as completed.
        :param tweet_id: The tweet ID of the associated completed work.
        """
        print("Marking work item (tweetid: %s) complete" % (tweet_id))
        row = self.get_one_row(self.work_queue_table, "tweetId", tweet_id)
        self.work_queue_table.update(row["id"], {"completed": True})

    def increment_attempts_queued_work(self, tweet_id):
        """ Increments the amount of times a piece of queued work as been attempted to be processed.
        :param tweet_id: The tweet ID of the associated completed work.
        :return: The newly-incremented number of attempts.
        """
        row = self.get_one_row(self.work_queue_table, "tweetId", tweet_id)
        attempts = row["fields"]["attempts"] + 1
        self.work_queue_table.update(row["id"], {"attempts": attempts})
        return attempts

    def increment_attempts_queued_work(self, tweet_id):
        """ Increments the amount of times a piece of queued work as been attempted to be processed.
        :param tweet_id: The tweet ID of the associated completed work.
        :return: The newly-incremented number of attempts.
        """
        row = self.get_one_row(self.work_queue_table, "tweetId", tweet_id)
        attempts = row["fields"]["attempts"] + 1
        self.work_queue_table.update(row["id"], {"attempts": attempts})
        return attempts

    def set_config(self, key, value):
        """ Persists the associated key with a value
        :param key: The key (string)
        :param value: The value (string)
        """
        existing_config = self.get_one_row(self.config_table, "config_name", key)
        if existing_config is None:
            self.config_table.create({"config_name": key, "value": value})
        else:
            self.config_table.update(existing_config["id"], {"value": value})

    def add_leaderboard_entry(self, playerName, scoringPayload, submission_url):
        """ Adds a leaderboard entry to the leaderboard table
        :param playerName: The name of the player
        :param scoringPayload: The payload received from the scoring service
        """
        print("adding leaderboard entry")
        expression = scoringPayload["expression"]
        gameplayUrl = scoringPayload["gameplay"]
        level = scoringPayload["level"]
        charCount = scoringPayload["charCount"]
        playURL = submission_url
        time = scoringPayload["time"]
        self.leaderboard_table.create(
            {"expression": expression, "time": time, "level": level, "playURL": playURL, "charCount": charCount,
             "player": playerName, "gameplay": gameplayUrl})

    def get_puzzle_data(self, puzzle_id):
        """ Returns the data associated with a given puzzle
        :param puzzle_id: The ID of the puzzle
        :return: Puzzle data or None
        """
        return self.get_one_row(self.puzzle_table, "id", puzzle_id)

    def get_submission_with_url(self, submission_url):
        """ Returns an entry from the leaderboard table that matches a given URL.  This is mainly used for checking
            for duplicate submissions.
        :param submission_url: The URL of a given submission
        :return: Leaderboard data associated with that URL, or None
        """
        return self.get_one_row(self.leaderboard_table, "playURL", submission_url)
