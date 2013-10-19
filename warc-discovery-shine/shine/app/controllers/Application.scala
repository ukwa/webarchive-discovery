package controllers

import play.api._
import play.api.mvc._
import scala.collection.JavaConverters._
import java.util.List
import uk.bl.wa.shine.SolrShine
import uk.bl.wa.shine.Query

object Application extends Controller {
  
  val config = play.Play.application().configuration().getConfig("shine");
  
  val solr = new SolrShine(config.getString("host"), config);
  
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }
  
  def halflife = Action {
    solr.halflife();
    Ok(views.html.index("Half-life..."))
  }
  
  def search ( query: String ) = Action { implicit request =>
    val map = request.queryString;
    val javaMap = map.map { case (k,v) => (k, v.asJava) }.asJava;
    val q = new Query()
    q.query = query
    q.parseParams(javaMap)
    q.res = solr.search(query, q.filters)
    Ok(views.html.search(q))
  }
  
}