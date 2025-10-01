from enum import Enum
import csv
import os
import json
from typing import Any

fcr_dir    = os.path.dirname(os.path.abspath(__file__))
fcr_config = os.path.join(fcr_dir, "config.conf")

class ConfTypes(Enum):
	KEY_VALUE = 1
	JSON = 2

class FileConfigReader:
	_config_cache: dict[tuple[str, ConfTypes], dict[str, Any]] = {}
	_tree_cache: dict[str, dict[str, list[str]]] = {}

	def __init__(self,
	             config_path: str = fcr_config,
	             conf_type: ConfTypes = ConfTypes.KEY_VALUE,
	             required_keys: list[str] | None = None,
	             force_refresh: bool = False):
		if force_refresh:
			self.invalidate_caches(config_path=config_path)

		self.config = self._get_config(config_path, conf_type, required_keys)
		root = self.config.get('root', None)
		if not root:
			raise KeyError("Missing 'root' in configuration.")
		self.root = os.path.abspath(root)
		self.tree = self._get_tree(self.root)

	@classmethod
	def _get_config(cls,
	                path: str,
	                conf_type: ConfTypes,
	                required_keys: list[str] | None) -> dict[str, Any]:
		if not os.path.exists(path):
			raise FileNotFoundError(f"Configuration file not found: {path}")
		key = (path, conf_type)
		mtime = os.path.getmtime(path)
		entry = cls._config_cache.get(key)
		if entry and entry.get("mtime") == mtime:
			return entry["config"]
		config = cls.load_config(path, conf_type, required_keys)
		cls._config_cache[key] = {"mtime": mtime, "config": config}
		return config

	@classmethod
	def _get_tree(cls, root_abs: str) -> dict[str, list[str]]:
		if root_abs in cls._tree_cache:
			return cls._tree_cache[root_abs]
		tree = cls.tree_scan(root_abs)
		cls._tree_cache[root_abs] = tree
		return tree

	@staticmethod
	def _depth(rel_path: str) -> int:
		rel_path = rel_path.strip(os.sep)
		return 1 if not rel_path else rel_path.count(os.sep) + 1

	@staticmethod
	def tree_scan(root_abs: str) -> dict[str, list[str]]:
		"""
		Build an index: filename -> list of *relative* paths (relative to root_abs),
		sorted deterministically by (depth, lexicographic path). This lets find()
		return the first match and terminate early.
		"""
		if not root_abs or not os.path.exists(root_abs):
			raise ValueError("Invalid root path provided for tree scan.")
		index: dict[str, list[str]] = {}
		for dirpath, _, filenames in os.walk(root_abs):
			filenames.sort()
			rel_dir = os.path.relpath(dirpath, root_abs)
			for filename in filenames:
				rel_path = filename if rel_dir == "." else os.path.normpath(os.path.join(rel_dir, filename))
				index.setdefault(filename, []).append(rel_path)
		for fname, paths in index.items():
			paths.sort(key=lambda p: (FileConfigReader._depth(p), p))
		return index

	def find(self,
	         name: str,
	         start: str | None = None,
	         parse_known_types: bool = True) -> dict[str, Any] | str | list[str]:
		"""
		Deterministic single-result lookup using the cached tree only.

		- name: filename, optionally with subdirs (e.g. "foo/bar.json")
		- start: subdirectory under root to scope the search (relative to root).
		         If name includes a subpath, it's combined with start.

		Returns file contents as str; if .json or .conf and parse_known_types=True,
		returns a dict. Raises FileNotFoundError if not found.
		"""
		if not name:
			raise ValueError("name must be provided.")

		name_norm = os.path.normpath(name)
		base = os.path.basename(name_norm)
		path_hint = os.path.dirname(name_norm)

		effective_start = os.path.normpath(
			os.path.join(start or "", path_hint)
		) if (start or path_hint) else ""

		candidates = self.tree.get(base)
		if not candidates:
			raise FileNotFoundError(f"'{name}' not found under root '{self.root}'.")

		if not effective_start or effective_start in (".", os.curdir):
			rel_path = candidates[0]
		else:
			prefix = effective_start.rstrip(os.sep)
			rel_path = None
			for rp in candidates:
				if rp == prefix or rp.startswith(prefix + os.sep):
					rel_path = rp
					break
			if rel_path is None:
				raise FileNotFoundError(
					f"'{name}' not found under scope '{effective_start}' (root '{self.root}')."
				)

		full_path = os.path.join(self.root, rel_path)
		ext = os.path.splitext(full_path)[1].lower()
		if parse_known_types and ext == ".json":
			with open(full_path, "r", encoding="utf-8") as f:
				return json.load(f)
		if parse_known_types and ext == ".conf":
			return self.load_config(full_path, ConfTypes.KEY_VALUE)
		if parse_known_types and ext == ".sql":
			return self.load_sql(full_path)
		if parse_known_types and ext == ".csv":
			return self.load_csv(full_path)

		with open(full_path, "r", encoding="utf-8", errors="replace") as f:
			return f.read()

	@staticmethod
	def load_json(path: str, required_keys: list[str] | None = None) -> dict[str, Any]:
		if not os.path.exists(path):
			raise FileNotFoundError(f"JSON file not found: {path}")

		with open(path, 'r', encoding='utf-8') as file:
			config = json.load(file)

		if required_keys:
			missing_keys = [key for key in required_keys if key not in config]
			if missing_keys:
				raise KeyError(f"Missing required JSON keys: {', '.join(missing_keys)}")

		return config

	@staticmethod
	def load_config(path: str,
					conf_type: ConfTypes = ConfTypes.KEY_VALUE,
					required_keys: list[str] | None = None) -> dict[str, Any]:
		if not os.path.exists(path):
			raise FileNotFoundError(f"Configuration file not found: {path}")

		if conf_type == ConfTypes.JSON:
			return FileConfigReader.load_json(path, required_keys)

		config: dict[str, Any] = {}
		if conf_type == ConfTypes.KEY_VALUE:
			with open(path, 'r', encoding='utf-8') as file:
				for line in file:
					line = line.strip()
					if line and not line.startswith('#'):
						key, value = line.split('=', 1)
						config[key.strip()] = value.strip()
		else:
			raise ValueError("Unsupported configuration type")

		if required_keys:
			missing_keys = [key for key in required_keys if key not in config]
			if missing_keys:
				raise KeyError(f"Missing required configuration keys: {', '.join(missing_keys)}")

		return config

	@staticmethod
	def load_sql(path: str) -> list[str]:
		if not os.path.exists(path):
			raise FileNotFoundError(f"SQL file not found: {path}")

		commands = []
		current_command = []

		with open(path, 'r', encoding='utf-8') as file:
			for line in file:
				line = line.strip()
				if line.startswith('--') or not line:
					continue
				current_command.append(line)
				if line.endswith(';'):
					commands.append(' '.join(current_command).strip())
					current_command = []

		if current_command:
			commands.append(' '.join(current_command).strip())

		return commands

	@staticmethod
	def load_csv(path: str,
	            has_header: bool = True,
	            delimiter: str | None = None,
	            required_columns: list[str] | None = None,
	            encoding: str = "utf-8",
	            errors: str = "strict") -> list[dict[str, Any]] | list[list[str]]:
		if not os.path.exists(path):
			raise FileNotFoundError(f"CSV file not found: {path}")

		delim = delimiter
		if delim is None:
			with open(path, "r", encoding=encoding, errors=errors, newline="") as f:
				sample = f.read(2048)
				f.seek(0)
				try:
					delim = csv.Sniffer().sniff(sample).delimiter
				except Exception:
					delim = ","

		with open(path, "r", encoding=encoding, errors=errors, newline="") as f:
			if has_header:
				reader = csv.DictReader(f, delimiter=delim)
				headers = reader.fieldnames or []
				if required_columns:
					missing = [c for c in required_columns if c not in headers]
					if missing:
						raise KeyError(f"Missing required CSV columns: {', '.join(missing)}")
				return [dict(row) for row in reader]
			else:
				reader = csv.reader(f, delimiter=delim)
				return [list(row) for row in reader]

	@classmethod
	def invalidate_caches(cls, *, config_path: str | None = None, root: str | None = None) -> None:
		if config_path is None and root is None:
			cls._config_cache.clear()
			cls._tree_cache.clear()
			return
		if config_path is not None:
			for key in list(cls._config_cache.keys()):
				if key[0] == config_path:
					cls._config_cache.pop(key, None)
		if root is not None:
			cls._tree_cache.pop(os.path.abspath(root), None)
