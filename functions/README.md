# Documentation Pages Functions

## Documentation search

Documentation uses the built-in keyword search. The AI Search dialog is no longer
loaded, and `api/docs-search.js` returns `410 Gone` without contacting a provider
or recording events. This also stops requests from already-open older pages.

Run `node scripts/docs_search_test.mjs` to verify the retired endpoint never
contacts a provider and the retained result helpers still validate links.
