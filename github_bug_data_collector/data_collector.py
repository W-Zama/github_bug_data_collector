from github import Github
import github
from datetime import datetime
import csv
import os


class DataCollector:
    # CSVのカラム名とGitHub APIのレスポンスのキーの対応
    COLUMN_MAP_OF_ISSUES = {
        "number": "_rawData.number",
        "title": "_rawData.title",
        "created_at": "_rawData.created_at",
        "updated_at": "_rawData.updated_at",
        "closed_at": "_rawData.closed_at",
        "creator_name": "_rawData.user.login",
        "assignees_num": "_rawData.assignees",
        "labels_num": "_rawData.labels",
        "state": "_rawData.state",
        "locked": "_rawData.locked",
        "comments_num": "_rawData.comments",
        "reactions_+1_num": "_rawData.reactions.+1",
        "reactions_-1_num": "_rawData.reactions.-1",
        "reactions_laugh_num": "_rawData.reactions.laugh",
        "reactions_hooray_num": "_rawData.reactions.hooray",
        "reactions_confused_num": "_rawData.reactions.confused",
        "reactions_heart_num": "_rawData.reactions.heart",
        "reactions_rocket_num": "_rawData.reactions.rocket",
        "reactions_eyes_num": "_rawData.reactions.eyes",
        "reactions_total_num": "_rawData.reactions.total_count",
    }

    COLUMN_MAP_OF_USERS = {
        "creator_company": "_rawData.company",
        "creator_followers_num": "_rawData.followers",
        "creator_following_num": "_rawData.following",
        "creator_public_repos_num": "_rawData.public_repos",
        "creator_public_gists_num": "_rawData.public_gists",
        "creator_created_at": "_rawData.created_at",
        "creator_updated_at": "_rawData.updated_at",
    }

    @classmethod
    def get_column_map_of_issues(cls):
        """column_map_of_issuesのゲッタ"""
        return cls.COLUMN_MAP_OF_ISSUES

    @classmethod
    def get_column_map_of_users(cls):
        """column_map_of_usersのゲッタ"""
        return cls.COLUMN_MAP_OF_USERS

    def __init__(self, access_token: str):
        """
        DataCollectorクラスの初期化メソッド。

        Parameters:
        - access_token (str): GitHub APIにアクセスするためのアクセストークン。
        """

        self.github = Github(auth=github.Auth.Token(access_token))

    def show_rate_limit(self):
        """リクエスト制限に関する情報を表示する"""
        print(
            f"Remaining request count: {f"{self.github.rate_limiting[0]} / {self.github.rate_limiting[1]}"}")
        print(
            f"Request reset time: {datetime.fromtimestamp(self.github.rate_limiting_resettime)}")

    def generate_csv(self, owner: str, repo_name: str, dir_path: str = ".", state: str = "all", labels: str | None = None, limit: int | None = None):
        """issuesに基づいたCSVを生成する"""

        repo = self.github.get_repo(f"{owner}/{repo_name}")

        # labelsがNoneの場合にはデフォルト値を使用
        issues = repo.get_issues(state=state, labels=labels or [])

        total_issues = issues.totalCount

        column_map_of_issues = self.get_column_map_of_issues()
        column_map_of_users = self.get_column_map_of_users()

        # CSVファイルの作成と書き込み
        path = os.path.join(dir_path, f"{owner}_{repo_name}_issues.csv")

        os.makedirs(dir_path, exist_ok=True)
        with open(os.path.join(dir_path, f"{owner}_{repo_name}_issues.csv"), "w", encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

            # ヘッダーの書き込み
            writer.writerow(list(column_map_of_issues.keys()) + list(column_map_of_users.keys()))

            for i, issue in enumerate(issues):
                if limit and i >= limit:
                    break
                print(f"Processing issue {i+1}/{total_issues}")

                # 各カラムの値を取得
                row = []
                for column, json_path in column_map_of_issues.items():
                    value = issue.__dict__
                    for attr in json_path.split('.'):
                        value = value[attr]

                    # リストの場合、要素数を取得
                    if isinstance(value, list):
                        value = len(value)

                    row.append(str(value))

                # user.loginの情報を取得
                user_info = self.github.get_user(issue.user.login).__dict__

                # 各カラムの値を取得
                for column, json_path in column_map_of_users.items():
                    value = user_info
                    for attr in json_path.split('.'):
                        value = value[attr]

                    # リストの場合、要素数を取得
                    if isinstance(value, list):
                        value = len(value)

                    row.append(str(value))

                # CSVに行を追加
                writer.writerow(row)

        self.show_rate_limit()
