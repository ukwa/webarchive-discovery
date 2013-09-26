package controllers

import play.api._
import play.api.mvc._

import uk.bl.wa.shine.SolrShine

object Application extends Controller {
  
  val solr = new SolrShine("http://localhost:8080/discovery/");
  
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }
  
  def search ( query: String ) = Action {
    Ok(views.html.search("Your query is: "+solr.search(query)) )
  }
  
}