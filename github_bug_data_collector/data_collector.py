from github import Github
import github
from datetime import datetime
import pandas as pd
import time
from functools import reduce
from typing import Optional
from tqdm import tqdm


class DataCollector:
    # CSVのカラム名とGitHub APIのレスポンスのキーの対応
    COLUMN_MAP_OF_ISSUES = {
        "number": ["number"],
        "title": ["title"],
        "created_at": ["created_at"],
        "updated_at": ["updated_at"],
        "closed_at": ["closed_at"],
        "creator_name": ["user", "login"],
        "assignees_num": ["assignees"],
        "labels_num": ["labels"],
        "state": ["state"],
        "locked": ["locked"],
        "author_association": ["author_association"],
        "comments_num": ["comments"],
        "reactions_+1_num": ["reactions", "+1"],
        "reactions_-1_num": ["reactions", "-1"],
        "reactions_laugh_num": ["reactions", "laugh"],
        "reactions_hooray_num": ["reactions", "hooray"],
        "reactions_confused_num": ["reactions", "confused"],
        "reactions_heart_num": ["reactions", "heart"],
        "reactions_rocket_num": ["reactions", "rocket"],
        "reactions_eyes_num": ["reactions", "eyes"],
        "reactions_total_num": ["reactions", "total_count"],
    }

    COLUMN_MAP_OF_USERS = {
        "creator_name": ["login"],
        "creator_company": ["company"],
        "creator_followers_num": ["followers"],
        "creator_following_num": ["following"],
        "creator_public_repos_num": ["public_repos"],
        "creator_public_gists_num": ["public_gists"],
        "creator_created_at": ["created_at"],
        "creator_updated_at": ["updated_at"],
    }

    @classmethod
    def get_column_map_of_issues(cls) -> dict[str, str]:
        """column_map_of_issuesのゲッタ"""
        return cls.COLUMN_MAP_OF_ISSUES

    @classmethod
    def get_column_map_of_users(cls) -> dict[str, str]:
        """column_map_of_usersのゲッタ"""
        return cls.COLUMN_MAP_OF_USERS

    @classmethod
    def get_column_names(cls) -> list[str]:
        """全てのカラム名の一覧を取得する"""
        return list(cls.COLUMN_MAP_OF_ISSUES.keys()) + list(cls.COLUMN_MAP_OF_USERS.keys())

    def __init__(self, access_token: str) -> None:
        """
        DataCollectorクラスの初期化メソッド。

        Parameters:
        - access_token (str): GitHub APIにアクセスするためのアクセストークン。
        """

        # データセットを取得した時間
        self.timestamp = datetime.now()

        # GitHubオブジェクトの生成
        self.github = Github(auth=github.Auth.Token(access_token))

    def set_timestamp(self) -> None:
        """timestampのセッタ"""
        self.timestamp = datetime.now()

    def show_rate_limit(self) -> None:
        """リクエスト制限に関する情報を表示する"""
        print(
            f"Remaining request count: {f'{self.github.rate_limiting[0]} / {self.github.rate_limiting[1]}'}")
        print(
            f"Request reset time: {datetime.fromtimestamp(self.github.rate_limiting_resettime)}")

    def check_limit_and_wait(self) -> None:
        """リクエスト制限を確認し，リクエストが可能でないなら可能になるまで待機する"""
        if self.github.rate_limiting[0] <= 0:
            reset_time = self.github.rate_limiting_resettime
            print("Request limit exceeded. Waiting for reset time...")

            while datetime.now().timestamp() < reset_time:
                sleep_time = max(0, reset_time - datetime.now().timestamp())
                time.sleep(sleep_time)

            print("Request limit reset. Resuming requests.")

    def convert_data_type(self, value: str | list) -> int | float | datetime | str:
        """与えられた文字列を，int，float，Datetimeの適切な型に変換する．変換できない場合は文字列のまま返す．リストの場合は要素数を返す"""

        # listの場合は要素数を返す
        if isinstance(value, list):
            return len(value)

        # intとして変換を試みる
        try:
            return int(value)
        except:
            pass

        # floatとして変換を試みる
        try:
            return float(value)
        except:
            pass

        # 日付として変換を試みる
        time_formats = ["%Y-%m-%dT%H:%M:%SZ"]
        for fmt in time_formats:
            try:
                return datetime.strptime(value, fmt)
            except:
                pass

        # どの変換にも成功しなければ文字列として返す
        return value

    def calculate_time_to_next_issue(self) -> None:
        """issuesのリストから，各Issueの前のIssueとの時間差を計算する"""
        self.df_issues["time_to_next_issue"] = - \
            self.df_issues["created_at"].diff()

    def generate_dataframe(self, owner: str, repo_name: str, limit: Optional[int] = None, **kwargs) -> pd.DataFrame:
        """issuesに基づいたDataFrameを生成する"""

        # timestampをセット
        self.set_timestamp()

        # issuesとusersのDataFrameを作成
        self.df_issues = pd.DataFrame(
            columns=list(self.get_column_map_of_issues().keys()))
        self.df_users = pd.DataFrame(
            columns=self.get_column_map_of_users().keys())

        # usersのセットを作成
        users = set()

        # リポジトリを取得
        repo = self.github.get_repo(f"{owner}/{repo_name}")

        # issuesを取得
        self.check_limit_and_wait()
        self.issues = repo.get_issues(**kwargs)

        if limit is not None:
            total_issues = limit
        else:
            total_issues = self.issues.totalCount

        # 全てのIssueをリストに追加
        all_issues = []
        for i, issue in enumerate(tqdm(self.issues, desc="Getting issues", total=total_issues)):
            self.check_limit_and_wait()

            # limitが指定されている場合は，その数だけ取得
            if limit is not None and i >= limit:
                break

            all_issues.append(issue)

        # issuesを取得
        row_dict_list = []
        for i, issue in enumerate(tqdm(all_issues, desc="Converting issues to DataFrame", total=total_issues)):

            row_dict = {}

            for column, json_path in self.get_column_map_of_issues().items():

                # json_pathの階層を再起的にたどる
                value = reduce(
                    lambda d, key: d[key], json_path, issue.raw_data)

                # データ型を適切に変換
                value = self.convert_data_type(value)

                row_dict[column] = value

            # user.loginの情報をsetに追加
            users.add(row_dict["creator_name"])

            row_dict_list.append(row_dict)

        self.df_issues = pd.concat(
            [self.df_issues, pd.DataFrame(row_dict_list)])

        # time_to_next_issue（前のIssueとの時間差）を取得
        self.calculate_time_to_next_issue()

        # usersの情報を取得
        total_users = len(users)

        # それぞれのユーザ情報を取得
        row_dict_list = []
        for i, user in enumerate(tqdm(users, desc="Getting user info", total=total_users)):
            self.check_limit_and_wait()

            user_info = self.github.get_user(user)

            row_dict = {}

            for column, json_path in self.get_column_map_of_users().items():
                # value = user_info.copy()

                # json_pathの階層を再起的にたどる
                value = reduce(
                    lambda d, key: d[key], json_path, user_info.raw_data)

                # データ型を適切に変換
                value = self.convert_data_type(value)

                row_dict[column] = value

            row_dict_list.append(row_dict)

        self.df_users = pd.concat([self.df_users, pd.DataFrame(row_dict_list)])

        # ユーザ情報をマージ
        self.df_all = pd.merge(self.df_issues, self.df_users, left_on="creator_name",
                               right_on="creator_name", how="left")

        return self.df_all
