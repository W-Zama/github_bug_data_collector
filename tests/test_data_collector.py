import unittest
import os
import github
import github.PaginatedList
from dotenv import load_dotenv
from github_bug_data_collector.data_collector import DataCollector

# .envファイルから環境変数を読み込む
load_dotenv()

class TestDataCollector(unittest.TestCase):
    def setUp(self):
        # アクセストークンを使用してDataCollectorインスタンスを作成
        access_token = os.getenv("ACCESS_TOKEN")
        self.collector = DataCollector(access_token)

    def test_get_user_info(self):
        # ユーザー情報取得のテスト
        user_info = self.collector.get_user_info("octocat")
        self.assertIsInstance(user_info, dict)
        self.assertEqual(user_info["_rawData"]["login"], "octocat")

    def test_get_repository_info(self):
        # リポジトリ情報取得のテスト
        repo_info = self.collector.get_repository_info("octocat", "Hello-World")
        self.assertIsInstance(repo_info, dict)
        self.assertEqual(repo_info["_rawData"]["full_name"], "octocat/Hello-World")

    def test_get_commit_history(self):
        # コミット履歴取得のテスト
        commits = self.collector.get_commit_history("octocat", "Hello-World")
        self.assertIsInstance(commits, github.PaginatedList.PaginatedList)

    def test_get_issues(self):
        # Issue取得のテスト
        issues = self.collector.get_issues("octocat", "Hello-World")
        self.assertIsInstance(issues, github.PaginatedList.PaginatedList)


if __name__ == "__main__":
    unittest.main()
