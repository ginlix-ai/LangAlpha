/** The chat route's chunk: the page, and the gallery preload Main runs beside
 * it. A plain module, since a component file that exports anything else loses
 * fast refresh. */
export { default } from './ChatAgent';
export { preloadChatPage } from './galleries';
