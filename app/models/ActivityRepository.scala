package models

import slick.jdbc.JdbcProfile
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import play.api.db.slick.DatabaseConfigProvider


case class Activity(id: String, activity: String, created_ts: String)

class ActivityRepository @Inject()(dbConfigProvider: DatabaseConfigProvider)(implicit ec: ExecutionContext) {
  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import profile.api._

  class ActivityTable(tag: Tag) extends Table[Activity](tag, "activity") {
    def id = column[String]("id", O.PrimaryKey)
    def activity = column[String]("activity")
    def created_ts = column[String]("created_ts")
    def * = (id, activity, created_ts) <> ((Activity.apply _).tupled, Activity.unapply)
  }

  val activities = TableQuery[ActivityTable]

  def add(id: String, activity: String, created_ts: String): Future[Activity] = db.run {
    (activities += Activity(id, activity, created_ts)).map(_ => (Activity(id, activity, created_ts)))
  }
}
