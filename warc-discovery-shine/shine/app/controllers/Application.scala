package controllers

import play.api._
import play.api.mvc._
import scala.collection.JavaConverters._
import java.util.List
import uk.bl.wa.shine.SolrShine
import uk.bl.wa.shine.Query
import uk.bl.wa.shine.Rescued

object Application extends Controller {
  
  val config = play.Play.application().configuration().getConfig("shine");
  val solrHost = config.getString("host");
  
  val solr = new SolrShine(solrHost, config);
  
  val rescued = new Rescued(solrHost, config);
  
  
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }
  
  def halflife = Action {
    rescued.halflife();
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