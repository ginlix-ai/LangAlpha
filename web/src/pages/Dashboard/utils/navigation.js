/**
 * Shared navigation for news/infoflow items.
 * News articles navigate to /news/:id, infoflow items to /detail/:indexNumber.
 */
export function navigateToNewsItem(navigate, item) {
  if (item.id) {
    navigate(`/news/${item.id}`);
  } else if (item.indexNumber) {
    navigate(`/detail/${item.indexNumber}`);
  }
}
