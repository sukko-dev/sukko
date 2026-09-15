// Entry point for the CodeMirror 6 bundle used by the Admin UI JSON rule editor.
import { EditorState } from "@codemirror/state";
import { EditorView, keymap, lineNumbers, highlightActiveLine } from "@codemirror/view";
import { defaultKeymap, historyKeymap, history } from "@codemirror/commands";
import { json, jsonParseLinter } from "@codemirror/lang-json";
import { lintGutter, linter } from "@codemirror/lint";
import { autocompletion, closeBrackets } from "@codemirror/autocomplete";
import { oneDark } from "@codemirror/theme-one-dark";

// Exported as CodeMirrorBundle.{EditorState, EditorView, ...}
export {
  EditorState,
  EditorView,
  keymap,
  lineNumbers,
  highlightActiveLine,
  defaultKeymap,
  historyKeymap,
  history,
  json,
  jsonParseLinter,
  lintGutter,
  linter,
  autocompletion,
  closeBrackets,
  oneDark,
};
