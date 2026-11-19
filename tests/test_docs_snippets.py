"""Check the pyfabric API used in Python fences without running live examples."""

import ast
import importlib
import inspect
import re
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from textwrap import dedent
from types import ModuleType
from typing import Self, get_args, get_origin
from unittest.mock import Mock

import pytest

from pyfabric.client.http import FabricClient
from pyfabric.workspace.workspaces import list_workspaces

ROOT = Path(__file__).resolve().parents[1]
MEMORY = ROOT / "src/pyfabric/claude_memory"
DOCS = [
    ROOT / "README.md",
    *sorted((ROOT / "docs").glob("*.md")),
    *sorted(MEMORY.glob("*.md")),
]
FENCE = re.compile(r"^ *(`{3,}|~{3,})(.*)$")
ELLIPSIS_PLACEHOLDER = "ellipsis placeholder"
STUB = object()


@dataclass(frozen=True)
class Snippet:
    path: Path
    line: int
    source: str

    def location(self, line: int = 1) -> str:
        return f"{self.path.relative_to(ROOT).as_posix()}:{self.line + line - 1}"


def python_fences(path: Path, markdown: str) -> list[Snippet]:
    found = []
    lines = markdown.splitlines(keepends=True)
    opening = ""
    language = ""
    for index, line in enumerate(lines):
        match = FENCE.match(line.rstrip("\r\n"))
        if not match:
            continue
        delimiter, info = match.groups()
        if not opening:
            if delimiter.startswith("`") and "`" in info:
                continue
            opening = delimiter
            language = info.strip().lower()
            start = index + 1
        elif delimiter.startswith(opening) and not info.strip():
            if language in {"py", "python"}:
                found.append(
                    Snippet(path, start + 1, dedent("".join(lines[start:index])))
                )
            opening = ""
    return found


def parse_snippet(snippet: Snippet) -> tuple[str, ast.Module | None]:
    source = snippet.source
    if "# docs-snippet: skip" in source:
        return "explicit skip", None
    if source.lstrip().startswith(("%", "!", "# MAGIC %")):
        return "notebook magic", None
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return "Python fragment", None
    if any(
        isinstance(node, ast.Constant) and node.value is Ellipsis
        for node in ast.walk(tree)
    ):
        return ELLIPSIS_PLACEHOLDER, tree
    mode = "network" if "# docs-snippet: network" in source else "static"
    return mode, tree


@dataclass(frozen=True)
class Instance:
    """The class behind a constructor or annotated value, without instantiation."""

    cls: type


@dataclass(frozen=True)
class ListResult:
    item: Instance


def is_pyfabric_module(name: str) -> bool:
    return name == "pyfabric" or name.startswith("pyfabric.")


class ApiCheck(ast.NodeVisitor):
    """Resolve common import, constructor and method examples without evaluating arguments."""

    def __init__(self, snippet: Snippet, *, check_signatures: bool = True):
        self.snippet = snippet
        self.names: dict[str, object] = {}
        self.check_signatures = check_signatures

    def fail(self, node: ast.AST, error: Exception) -> None:
        pytest.fail(f"{self.snippet.location(node.lineno)}: {error}", pytrace=False)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if is_pyfabric_module(alias.name):
                try:
                    module = importlib.import_module(alias.name)
                    root = alias.name.split(".")[0]
                    self.names[alias.asname or root] = (
                        module if alias.asname else importlib.import_module(root)
                    )
                except ImportError as error:
                    self.fail(node, error)
            else:
                self.names[alias.asname or alias.name.split(".")[0]] = STUB

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level == 0 and node.module and is_pyfabric_module(node.module):
            try:
                module = importlib.import_module(node.module)
                for alias in node.names:
                    value = getattr(module, alias.name, STUB)
                    if value is STUB:
                        value = importlib.import_module(f"{node.module}.{alias.name}")
                    self.names[alias.asname or alias.name] = value
            except ImportError as error:
                self.fail(node, error)
        else:
            for alias in node.names:
                self.names[alias.asname or alias.name] = STUB

    def resolve(self, node: ast.AST) -> object:
        if isinstance(node, ast.Name):
            return self.names.get(node.id, STUB)
        if isinstance(node, ast.Call):
            target = self.resolve(node.func)
            if isinstance(target, type):
                return Instance(target)
            if inspect.isfunction(target) or inspect.ismethod(target):
                annotated = inspect.signature(target).return_annotation
                if isinstance(annotated, str):
                    annotated = annotated.strip("\"'")
                    # Resolve the documented list[Class] form without evaluating annotations.
                    collection = re.fullmatch(r"list\[\s*(\w+)\s*\]", annotated)
                    if collection:
                        item_type = target.__globals__.get(collection[1], STUB)
                        annotated = (
                            list[item_type] if isinstance(item_type, type) else STUB
                        )
                    else:
                        annotated = target.__globals__.get(annotated, STUB)
                if get_origin(annotated) is list:
                    item_types = get_args(annotated)
                    if len(item_types) == 1:
                        (item_type,) = item_types
                        if isinstance(item_type, type) and is_pyfabric_module(
                            item_type.__module__
                        ):
                            return ListResult(Instance(item_type))
                if annotated is Self and isinstance(node.func, ast.Attribute):
                    return self.resolve(node.func.value)
                if isinstance(annotated, type) and is_pyfabric_module(
                    annotated.__module__
                ):
                    return Instance(annotated)
            return STUB
        if isinstance(node, ast.Attribute):
            owner = self.resolve(node.value)
            try:
                if isinstance(owner, Instance):
                    if is_dataclass(owner.cls) and any(
                        field.name == node.attr for field in fields(owner.cls)
                    ):
                        return STUB
                    member = inspect.getattr_static(owner.cls, node.attr)
                    if isinstance(member, (classmethod, staticmethod)):
                        return getattr(owner.cls, node.attr)
                    if inspect.isfunction(member):
                        # Bind the receiver without constructing the documented client.
                        return member.__get__(STUB, owner.cls)
                    return member
                if isinstance(owner, (ModuleType, type)):
                    return getattr(owner, node.attr)
            except AttributeError as error:
                self.fail(node, error)
        return STUB

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self.resolve(node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        target = self.resolve(node.func)
        if self.check_signatures and callable(target):
            try:
                sig = inspect.signature(target)
            except ValueError:
                pass  # Some types expose no inspectable signature.
            else:
                args = [STUB for arg in node.args if not isinstance(arg, ast.Starred)]
                kwargs = {kw.arg: STUB for kw in node.keywords if kw.arg is not None}
                unpacking = len(args) != len(node.args) or len(kwargs) != len(
                    node.keywords
                )
                try:
                    bind = sig.bind_partial if unpacking else sig.bind
                    bind(*args, **kwargs)
                except TypeError as error:
                    self.fail(node, TypeError(f"{ast.unparse(node.func)}: {error}"))
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        for target in node.targets:
            self.bind(target, self.resolve(node.value))

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.generic_visit(node)
        annotation = self.resolve(node.annotation)
        if isinstance(annotation, type):
            self.bind(node.target, Instance(annotation))
        elif node.value is not None:
            self.bind(node.target, self.resolve(node.value))

    def visit_With(self, node: ast.With | ast.AsyncWith) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars:
                self.bind(item.optional_vars, self.resolve(item.context_expr))
        for statement in node.body:
            self.visit(statement)

    visit_AsyncWith = visit_With

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.iter)
        iterable = self.resolve(node.iter)
        self.bind(
            node.target, iterable.item if isinstance(iterable, ListResult) else STUB
        )
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        names = self.names.copy()
        self.generic_visit(node)
        self.names = names

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        names = self.names.copy()
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
            annotation = self.resolve(arg.annotation)
            self.names[arg.arg] = (
                Instance(annotation) if isinstance(annotation, type) else STUB
            )
        self.generic_visit(node)
        self.names = names

    visit_AsyncFunctionDef = visit_FunctionDef

    def bind(self, target: ast.AST, value: object) -> None:
        if isinstance(target, ast.Name):
            self.names[target.id] = value


def snippets(path: Path) -> list[Snippet]:
    return python_fences(path, path.read_text(encoding="utf-8"))


def check_snippet(snippet: Snippet) -> None:
    mode, tree = parse_snippet(snippet)
    if tree is not None:
        ApiCheck(snippet, check_signatures=mode != ELLIPSIS_PLACEHOLDER).visit(tree)
    if tree is None or mode == ELLIPSIS_PLACEHOLDER:
        pytest.skip(f"{snippet.location()}: {mode}")


@pytest.mark.parametrize(
    "snippet",
    [snippet for path in DOCS for snippet in snippets(path)],
    ids=lambda snippet: snippet.location(),
)
def test_pyfabric_snippet_api(snippet):
    check_snippet(snippet)


def workspace_listing() -> Snippet:
    (snippet,) = (
        snippet
        for snippet in snippets(MEMORY / "pyfabric.md")
        if snippet.source.startswith("# List workspaces\n")
    )
    return snippet


def run_workspace_listing(snippet: Snippet, fake: Mock) -> None:
    expected_setup = ast.parse(
        "from pyfabric.client.http import FabricClient\n"
        "from pyfabric.workspace.workspaces import list_workspaces\n"
        "client = FabricClient(cred)\n"
    ).body
    tree = ast.parse(snippet.source)
    match tree.body:
        case [
            *setup,
            ast.For(
                target=ast.Name(id="ws"),
                iter=ast.Call(
                    func=ast.Name(id="list_workspaces"),
                    args=[ast.Name(id="client")],
                    keywords=[],
                ),
                body=[
                    ast.Expr(
                        value=ast.Call(
                            func=ast.Name(id="print"), args=[_, _] as args, keywords=[]
                        )
                    )
                ],
                orelse=[],
            ) as loop,
        ] if [ast.dump(statement) for statement in setup] == [
            ast.dump(statement) for statement in expected_setup
        ]:
            for arg in args:
                match arg:
                    case (
                        ast.Subscript(
                            value=ast.Name(id="ws"), slice=ast.Constant(value=str())
                        )
                        | ast.Attribute(value=ast.Name(id="ws"))
                    ):
                        pass  # Let the real dictionary rows catch attribute access.
                    case _:
                        pytest.fail(
                            f"{snippet.location(arg.lineno)}: unexpected workspace listing field"
                        )
        case _:
            pytest.fail(
                f"{snippet.location()}: unexpected workspace listing setup or loop"
            )

    # Run only the checked loop. Bind the real helper and a fake client explicitly.
    tree.body = [loop]
    ast.increment_lineno(tree, snippet.line - 1)
    exec(
        compile(tree, str(snippet.path), "exec"),
        {
            "__builtins__": {},
            "client": fake,
            "list_workspaces": list_workspaces,
            "print": print,
        },
    )


def test_workspace_listing_prints_dict_fields(capsys):
    fake = Mock(spec=FabricClient)
    fake.get_paged.return_value = [{"id": "demo-id", "displayName": "Demo workspace"}]
    run_workspace_listing(workspace_listing(), fake)
    fake.get_paged.assert_called_once_with("workspaces")
    assert capsys.readouterr().out == "demo-id Demo workspace\n"


def test_workspace_listing_rejects_unapproved_prefix():
    snippet = workspace_listing()
    changed = Snippet(
        snippet.path,
        snippet.line,
        snippet.source.replace("for ws in", "client.get('sentinel')\nfor ws in"),
    )
    fake = Mock(spec=FabricClient)
    fake.get_paged.return_value = []
    try:
        with pytest.raises(pytest.fail.Exception, match="unexpected workspace listing"):
            run_workspace_listing(changed, fake)
    finally:
        fake.get.assert_not_called()
    fake.get_paged.assert_not_called()


def test_workspace_listing_rejects_dict_attribute_access():
    snippet = workspace_listing()
    changed = Snippet(
        snippet.path, snippet.line, snippet.source.replace('ws["id"]', "ws.id")
    )
    fake = Mock(spec=FabricClient)
    fake.get_paged.return_value = [{"id": "demo-id", "displayName": "Demo workspace"}]
    with pytest.raises(AttributeError, match="'dict' object has no attribute 'id'"):
        run_workspace_listing(changed, fake)


@pytest.mark.parametrize(
    ("file", "body", "line", "error"),
    [
        (
            "README.md",
            "from pyfabric.workspace.workspaces import renamed",
            4,
            "renamed",
        ),
        (
            "docs/api.md",
            "from pyfabric.client.http import FabricClient\nFabricClient().renamed()",
            5,
            "renamed",
        ),
        (
            "docs/api.md",
            "from pyfabric.client.http import FabricClient\nFabricClient().get_paged(bad=True)",
            5,
            "get_paged",
        ),
        (
            "docs/api.md",
            "from pyfabric.items.notebook import NotebookBuilder\n"
            "NotebookBuilder().add_python('x').renamed()",
            5,
            "renamed",
        ),
    ],
)
def test_stale_snippet_reports_source_location(file, body, line, error):
    (snippet,) = python_fences(ROOT / file, f"Heading\n\n```python\n{body}\n```\n")
    _, tree = parse_snippet(snippet)
    assert tree is not None
    with pytest.raises(pytest.fail.Exception, match=rf"{file}:{line}: .*{error}"):
        ApiCheck(snippet).visit(tree)


def test_docs_without_python_fences():
    assert python_fences(ROOT / "README.md", "Text\n```bash\npytest\n```\n") == []


@pytest.mark.parametrize("marker", ["`", "~"])
def test_python_fence_accepts_longer_closing_delimiter(marker):
    path = ROOT / "README.md"
    source = f"Title\n{marker * 3}python\n    print('hello')\n{marker * 4}\n"
    assert python_fences(path, source) == [Snippet(path, 3, "print('hello')\n")]


def test_from_import_shadows_pyfabric_alias():
    snippet = Snippet(
        ROOT / "README.md",
        4,
        "from pyfabric.client.http import FabricClient as Path\n"
        "from pathlib import Path\n"
        "Path('demo').read_text()\n",
    )
    check_snippet(snippet)


def test_class_binding_preserves_outer_client():
    snippet = Snippet(
        ROOT / "README.md",
        4,
        "from pyfabric.client.http import FabricClient\n"
        "class Example:\n"
        "    FabricClient = None\n"
        "FabricClient().renamed()\n",
    )
    with pytest.raises(pytest.fail.Exception, match=r"README\.md:7: .*renamed"):
        check_snippet(snippet)


@pytest.mark.parametrize(
    ("source", "line", "missing_name"),
    [
        (
            "from pyfabric.items.notebook import RenamedBuilder\nRenamedBuilder(...)\n",
            4,
            "RenamedBuilder",
        ),
        (
            "from pyfabric.items.notebook import NotebookBuilder\n"
            "NotebookBuilder().renamed(...)\n",
            5,
            "renamed",
        ),
    ],
)
def test_placeholder_snippet_still_checks_names(source, line, missing_name):
    snippet = Snippet(ROOT / "README.md", 4, source)
    with pytest.raises((pytest.fail.Exception, pytest.skip.Exception)) as outcome:
        check_snippet(snippet)
    assert isinstance(outcome.value, pytest.fail.Exception)
    assert re.search(rf"README\.md:{line}: .*{missing_name}", str(outcome.value))


def test_placeholder_snippet_visibly_skips_signature_binding():
    snippet = Snippet(
        ROOT / "README.md",
        4,
        "from pyfabric.items.notebook import NotebookBuilder\n"
        "NotebookBuilder().add_python(placeholder=...)\n",
    )
    with pytest.raises(
        pytest.skip.Exception, match=r"README\.md:4: ellipsis placeholder"
    ):
        check_snippet(snippet)


@pytest.mark.parametrize(
    ("module", "call", "field"),
    [
        ("pyfabric.items.validate", "validate_workspace(path)", "item_path"),
        ("pyfabric.deploy", "publish_repo(client, ws_id, path)", "item_id"),
    ],
)
def test_snippet_checks_fields_on_loop_results(module, call, field):
    source = (
        f"from {module} import {call.split('(')[0]}\n"
        f"results = {call}\n"
        "for result in results:\n"
        f"    print(result.{field})\n"
    )
    check_snippet(Snippet(ROOT / "README.md", 4, source))
    stale = Snippet(ROOT / "README.md", 4, source.replace(field, "removed_field"))
    with pytest.raises(pytest.fail.Exception, match=r"README\.md:7: .*removed_field"):
        check_snippet(stale)


@pytest.mark.parametrize(
    ("source", "mode"),
    [
        ("%pip install pyfabric\n", "notebook magic"),
        ("from pyfabric import ...\n", "Python fragment"),
        ("FabricClient(...)\n", "ellipsis placeholder"),
        ("# docs-snippet: skip\nimport pyfabric.missing\n", "explicit skip"),
        (
            "# docs-snippet: network\nfrom pyfabric.client.http import FabricClient\n"
            "FabricClient().get_paged('workspaces')\n",
            "network",
        ),
    ],
)
def test_snippet_classification(source, mode, monkeypatch):
    snippet = Snippet(ROOT / "README.md", 1, source)
    actual, tree = parse_snippet(snippet)
    assert actual == mode
    if mode == "network":
        assert tree is not None
        request = Mock(side_effect=AssertionError("unexpected network call"))
        monkeypatch.setattr(FabricClient, "get_paged", request)
        ApiCheck(snippet).visit(tree)
        request.assert_not_called()
    elif mode == ELLIPSIS_PLACEHOLDER:
        assert tree is not None
    else:
        assert tree is None


def test_retirement_decisions_have_reasons() -> None:
    expected = {
        "src/pyfabric/workspace/workspaces.py",
        "src/pyfabric/items/crud.py",
        "src/pyfabric/items/jobs.py",
        "src/pyfabric/deploy.py",
    }
    document = (ROOT / "docs/ecosystem.md").read_text(encoding="utf-8")
    decisions = {}
    for line in document.splitlines():
        columns = [column.strip() for column in line.strip().strip("|").split("|")]
        if len(columns) != 3:
            continue
        surface = columns[0].strip("`")
        if surface in expected:
            decisions[surface] = columns[1:]
    assert decisions.keys() == expected, "Every retirement candidate needs a decision"
    for surface, (decision, reason) in decisions.items():
        assert decision in {"Keep", "Deprecate", "Remove"}, surface
        assert reason, f"Missing retirement rationale for {surface}"
