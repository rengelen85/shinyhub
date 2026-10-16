// Deterministic, syntax-aware identifier renaming; no optimization or property mangling.
import { minify } from 'terser';

let source = '';
for await (const chunk of process.stdin) source += chunk;
const result = await minify(source, {
  compress: false,
  mangle: {
    toplevel: true,
    nth_identifier: { get: index => 'v' + index.toString(36) },
  },
  format: { comments: /^!/ },
});
process.stdout.write(result.code);
