def severity($level):
  if $level == "error" then
    "ERROR"
  elif $level == "warning" then
    "WARNING"
  else
    "INFO"
  end;

def primary_span($message):
  (
    $message.spans // []
    | map(select(.is_primary and (.file_name // "") != "" and (.file_name // "") != "<command line>"))
    | .[0]
  ) // (
    $message.spans // []
    | map(select((.file_name // "") != "" and (.file_name // "") != "<command line>"))
    | .[0]
  );

{
  diagnostics: [
    inputs
    | select(.reason == "compiler-message")
    | .message as $message
    | primary_span($message) as $span
    | select($span != null)
    | {
        message: ($message.rendered // $message.message),
        location: {
          path: $span.file_name,
          range: {
            start: {
              line: ($span.line_start // 1),
              column: ($span.column_start // 1)
            },
            end: {
              line: ($span.line_end // ($span.line_start // 1)),
              column: ($span.column_end // ($span.column_start // 1))
            }
          }
        },
        severity: severity($message.level),
        source: {
          name: "cargo clippy"
        }
      }
      + (
        if ($message.code.code // "") != "" then
          {
            code: {
              value: $message.code.code
            }
          }
        else
          {}
        end
      )
  ]
}
