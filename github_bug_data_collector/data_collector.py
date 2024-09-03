from github import Github
import github


class DataCollector:
    def __init__(self, access_token):
        """
        DataCollectorクラスの初期化メソッド。

        Parameters:
        - access_token (str): GitHub APIにアクセスするためのアクセストークン。
        """
        self.github = Github(auth=github.Auth.Token(access_token))

    def get_user_info(self, username):
        """
        指定されたユーザーの情報を取得します。

        Parameters:
        - username (str): GitHubユーザー名。

        Returns:
        - dict: ユーザー情報を含む辞書。
        """
        user = self.github.get_user(username)
        return user.__dict__

    def get_repository_info(self, owner, repo_name):
        """
        指定されたリポジトリの情報を取得します。

        Parameters:
        - owner (str): リポジトリの所有者のユーザー名または組織名。
        - repo_name (str): リポジトリの名前。

        Returns:
        - dict: リポジトリ情報を含む辞書。
        """
        repo = self.github.get_repo(f"{owner}/{repo_name}")
        return repo.__dict__

    def get_commit_history(self, owner, repo_name, branch="main"):
        """
        指定されたリポジトリのコミット履歴を取得します。

        Parameters:
        - owner (str): リポジトリの所有者のユーザー名または組織名。
        - repo_name (str): リポジトリの名前。
        - branch (str): コミット履歴を取得するブランチ名。デフォルトは "main"。

        Returns:
        - PaginatedList: コミット履歴を含むリスト。
        """
        repo = self.github.get_repo(f"{owner}/{repo_name}")
        commits = repo.get_commits(sha=branch)
        return commits

    def get_issues(self, owner, repo_name, state="open"):
        """
        指定されたリポジトリのIssueを取得します。

        Parameters:
        - owner (str): リポジトリの所有者のユーザー名または組織名。
        - repo_name (str): リポジトリの名前。
        - state (str): Issueの状態（"open", "closed", "all"）。デフォルトは "open"。

        Returns:
        - PaginatedList: Issue情報を含むリスト。
        """
        repo = self.github.get_repo(f"{owner}/{repo_name}")
        issues = repo.get_issues(state=state)
        return issues
