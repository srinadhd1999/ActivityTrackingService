package controllers

import models.{Activity, ActivityRepository}
import play.api.libs.json.{Format, Json}
import play.api.mvc._
import services.KafkaMessageConsumer
import javax.inject.Inject
import scala.concurrent.ExecutionContext


class ActivityController @Inject() (activityRepository: ActivityRepository, kafkaMessageConsumer: KafkaMessageConsumer, cc: ControllerComponents)(implicit ec: ExecutionContext) extends AbstractController(cc) {
  implicit val userFormat: Format[Activity] = Json.format[Activity]
  println("In Activity Controller")
  kafkaMessageConsumer.receiveMessages()

  def index() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index())
  }
}
