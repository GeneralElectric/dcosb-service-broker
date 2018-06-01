package io.predix.dcosb.servicebroker

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.predix.dcosb.servicemodule.api.util.ServiceLoader
import akka.pattern.ask
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.predix.dcosb.dcos.DCOSProxy
import io.predix.dcosb.util.actor.ConfiguredActor._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  * An executable object that:
  * - brings up an Actor System,
  * - starts the ServiceLoader, and instructs it to attempt to load
  * configured ( per dcosb.services ) ServiceModule implementations.
  * - starts the OpenServiceBrokerApi, asks for a Route to be generated
  * from the loaded service objects in ServiceLoader
  * - binds to a configured ( per dcosb.service-broker ) listening socket
  * and registers the Route from the OpenServiceBrokerApi as a handler
  *
  */
object Daemon extends App {

  val tConfig = ConfigFactory.load()

  // create ActorSystem
  implicit val system = ActorSystem("service-broker-daemon")
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(FiniteDuration(5, TimeUnit.SECONDS))
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))

  val childMaker = (f: ActorRefFactory, a: Class[_ <: Actor], name: String) => {
    f.actorOf(Props(a), name)
  }

  val httpClientFactory: DCOSProxy.HttpClientFactory = { connection => {

      val clientFlow = Http(system).cachedHostConnectionPool[String](connection.apiHost, connection.apiPort)
      (request: HttpRequest, context:String) => {
        Source.single(request -> context)
          .via(clientFlow)
          .runWith(Sink.head).flatMap {
          case (Success(r: HttpResponse), c: String) => Future.successful((r, c))
          case (Failure(f), _) => Future.failed(f)
        }

      }

    }
  }

  // start & configure ServiceLoader
  val serviceLoader =
    childMaker(system, classOf[ServiceLoader], ServiceLoader.name)
  (serviceLoader ? ServiceLoader.Configuration(childMaker, httpClientFactory)) onComplete {
    case Success(Success(_)) =>
      // iterate through dcosb.services & send RegisterService messages
      val serviceActors = (tConfig
        .getObject("dcosb.services")
        .asScala map {
        case (serviceId, _) =>
          val implementingClass = tConfig.getString(
            s"dcosb.services.${serviceId}.implementation")

          system.log.debug(
            s"Trying to register ServiceModule($serviceId,$implementingClass)")

          (serviceLoader ? ServiceLoader.RegisterService(
            serviceId,
            implementingClass)) map {
            case Success(actor: ActorRef) =>
              system.log.warning(
                s"Registered ServiceModule($serviceId,$implementingClass) as $actor")

              Some(actor)
            case e =>
              system.log.error(
                s"Failed to register ServiceModule($serviceId,$implementingClass), skipping! $e")
              None
          }

      }).toList

      Future.sequence(serviceActors) onComplete {
        case Success(actors) =>
          // get total registered service modules from ServiceLoader
          (serviceLoader ? ServiceLoader.GetServices()) onComplete {
            case Success(
                ServiceLoader.Services(
                  registeredServices: ServiceLoader.ServiceList)) =>
              system.log.debug(
                s"Completed registering ServiceModule implementations: $registeredServices")

              // get route from OpenServiceBroker
              val broker = childMaker(system,
                                      classOf[OpenServiceBrokerApi],
                                      OpenServiceBrokerApi.name)
              (broker ? OpenServiceBrokerApi.Configuration(childMaker)) onComplete {
                case Success(Success(_)) =>
                  (broker ? OpenServiceBrokerApi.RouteForServiceModules(
                    registeredServices)) onComplete {
                    case Success(route: Route) =>
                      system.log.debug(s"Created Route: $route")

                      implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system))
                      // create listening socket, bind route
                      Http(system)
                        .bindAndHandle(
                          route.asInstanceOf[Route],
                          tConfig.getString(
                            "dcosb.service-broker.listen.address"),
                          tConfig.getInt(
                            "dcosb.service-broker.listen.port")) onComplete {

                        case Success(Http.ServerBinding(a)) =>
                          system.log.warning(s"Open Service Broker listening at $a")
                        case Failure(e: Throwable) =>
                          system.log.error(s"Failed to bind Open Service Broker: $e")
                      }
                  }
              }

            case r =>
              system.log.error(
                s"Failed to retrieve registered services from ServiceLoader: $r")
              bail()

          }

        case Failure(e: Throwable) =>
          system.log.error(
            s"Failed to register ServiceModule implementations: $e")
          bail()

      }

    case r =>
      system.log.error(s"Failed to configure ServiceLoader, shutting down: $r")
      bail()

  }

  def bail() = {
    system.terminate() onComplete { _ =>
      System.exit(1)
    }
  }

}
