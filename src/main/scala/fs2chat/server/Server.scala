package fs2chat
package server

import cache.MsgCounter
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import cats.{FlatMap, MonadError}
import com.comcast.ip4s.Port
import fs2.Stream
import fs2.io.tcp.{Socket, SocketGroup}
import io.chrisdavenport.log4cats.Logger
import java.net.InetSocketAddress
import java.util.UUID

object Server {

  /**
    * Represents a client that has connected to this server.
    *
    * After connecting, clients must send a [[Protocol.ClientCommand.RequestUsername]]
    * message, requesting a username. The server will either accept that request or
    * in the event that the request username is taken, will assign a unique username.
    * Until this exchange has completed, the `username` field is `None` and the client
    * will receive no messages or alerts from the server.
    */
  private case class ConnectedClient[F[_]](
      id: UUID,
      username: Option[Username],
      messageSocket: MessageSocket[F,
                                   Protocol.ClientCommand,
                                   Protocol.ServerCommand])

  private object ConnectedClient {
    def apply[F[_]: Concurrent](socket: Socket[F]): F[ConnectedClient[F]] =
      for {
        id <- Sync[F].delay(UUID.randomUUID)
        messageSocket <- MessageSocket(socket,
                                       Protocol.ClientCommand.codec,
                                       Protocol.ServerCommand.codec,
                                       1024)
      } yield ConnectedClient(id, None, messageSocket)
  }

  private class Clients[F[_]: Sync](
      ref: Ref[F, Map[UUID, ConnectedClient[F]]]) {
    def get(id: UUID): F[Option[ConnectedClient[F]]] = ref.get.map(_.get(id))
    def all: F[List[ConnectedClient[F]]] = ref.get.map(_.values.toList)
    def named: F[List[ConnectedClient[F]]] =
      ref.get.map(_.values.toList.filter(_.username.isDefined))
    def register(state: ConnectedClient[F]): F[Unit] =
      ref.update { oldClients =>
        oldClients + (state.id -> state)
      }
    def unregister(id: UUID): F[Option[ConnectedClient[F]]] =
      ref.modify { old =>
        (old - id, old.get(id))
      }
    def setUsername(clientId: UUID, username: Username): F[Username] =
      ref.modify { clientsById =>
        val usernameToSet =
          determineUniqueUsername(clientsById - clientId, username)
        val updatedClient =
          clientsById.get(clientId).map(_.copy(username = Some(usernameToSet)))
        val updatedClients = updatedClient
          .map(c => clientsById + (clientId -> c))
          .getOrElse(clientsById)
        (updatedClients, usernameToSet)
      }

    def broadcast(cmd: Protocol.ServerCommand): F[Unit] =
      named.flatMap(_.traverse_(_.messageSocket.write1(cmd)))
  }

  private object Clients {
    def apply[F[_]: Sync]: F[Clients[F]] =
      Ref[F]
        .of(Map.empty[UUID, ConnectedClient[F]])
        .map(ref => new Clients(ref))
  }

  def start[F[_]: Concurrent: ContextShift: Logger](socketGroup: SocketGroup,
                                                    port: Port,
                                                    msgCounter: MsgCounter[F]) =
    Stream.eval_(Logger[F].info(s"Starting server on port $port")) ++
      Stream
        .eval(Clients[F])
        .flatMap { clients =>
          socketGroup.server[F](new InetSocketAddress(port.value)).map {
            clientSocketResource =>
              def unregisterClient(state: ConnectedClient[F]) =
                clients.unregister(state.id).flatMap { client =>
                  client
                    .flatMap(_.username)
                    .traverse_(username =>
                      clients.broadcast(Protocol.ServerCommand.Alert(
                        s"$username disconnected.")))
                } *> Logger[F].info(s"Unregistered client ${state.id}")
              Stream
                .resource(clientSocketResource)
                .flatMap { clientSocket =>
                  Stream
                    .bracket(ConnectedClient[F](clientSocket).flatTap(
                      clients.register))(unregisterClient)
                    .flatMap { client =>
                      handleClient[F](clients, client, clientSocket, msgCounter)
                    }
                }
                .scope
          }
        }
        .parJoinUnbounded

  private def handleClient[F[_]: Concurrent: Logger](
      clients: Clients[F],
      clientState: ConnectedClient[F],
      clientSocket: Socket[F],
      msgCounter: MsgCounter[F]): Stream[F, Nothing] = {
    logNewClient(clientState, clientSocket) ++
      Stream.eval_(
        clientState.messageSocket.write1(
          Protocol.ServerCommand.Alert("Welcome to FS2 Chat!"))) ++
      processIncoming(clients,
                      clientState.id,
                      msgCounter,
                      clientState.messageSocket)
  }.handleErrorWith {
    case _: UserQuit =>
      Stream.eval_(
        Logger[F].info(s"Client quit ${clientState.id}") *> clientSocket.close)
    case err =>
      Stream.eval_(Logger[F].error(
        s"Fatal error for client ${clientState.id} - $err") *> clientSocket.close)
  }

  private def logNewClient[F[_]: FlatMap: Logger](
      clientState: ConnectedClient[F],
      clientSocket: Socket[F]): Stream[F, Nothing] =
    Stream.eval_(clientSocket.remoteAddress.flatMap { clientAddress =>
      Logger[F].info(s"Accepted client ${clientState.id} on $clientAddress")
    })

  private def processIncoming[F[_]](
      clients: Clients[F],
      clientId: UUID,
      msgCounter: MsgCounter[F],
      messageSocket: MessageSocket[F,
                                   Protocol.ClientCommand,
                                   Protocol.ServerCommand])(
      implicit F: MonadError[F, Throwable]): Stream[F, Nothing] =
    messageSocket.read.evalMap {
      case Protocol.ClientCommand.RequestUsername(username) =>
        clients.setUsername(clientId, username).flatMap { nameToSet =>
          val alertIfAltered =
            if (username =!= nameToSet)
              messageSocket.write1(
                Protocol.ServerCommand.Alert(
                  s"$username already taken, name set to $nameToSet"))
            else F.unit
          alertIfAltered *> messageSocket.write1(
            Protocol.ServerCommand.SetUsername(nameToSet)) *>
            clients.broadcast(
              Protocol.ServerCommand.Alert(s"$nameToSet connected.")) *>
            msgCounter
              .get(nameToSet)
              .flatMap {
                case None =>
                  clients.broadcast(Protocol.ServerCommand.Alert(
                    s"$nameToSet is joining the chat for the first time ever!"))
                case Some(n) =>
                  clients.broadcast(Protocol.ServerCommand.Alert(
                    s"$nameToSet has previously sent #$n messages."))
              }
        }
      case Protocol.ClientCommand.SendMessage(message) =>
        if (message.startsWith("/")) {
          val cmd = message.tail.toLowerCase
          cmd match {
            case "users" =>
              val usernames = clients.named.map(_.flatMap(_.username).sorted)
              usernames.flatMap(
                users =>
                  messageSocket.write1(
                    Protocol.ServerCommand.Alert(users.mkString(", "))))
            case "quit" =>
              messageSocket.write1(Protocol.ServerCommand.Disconnect) *>
                F.raiseError(new UserQuit): F[Unit]
            case _ =>
              messageSocket.write1(
                Protocol.ServerCommand.Alert("Unknown command"))
          }
        } else {
          clients.get(clientId).flatMap {
            case Some(client) =>
              client.username match {
                case None =>
                  F.unit // Ignore messages sent before username assignment
                case Some(username) =>
                  val cmd = Protocol.ServerCommand.Message(username, message)
                  clients.broadcast(cmd) *> msgCounter.incr(username)
              }
            case None => F.unit
          }
        }
    }.drain

  private def determineUniqueUsername[F[_]](
      clients: Map[UUID, ConnectedClient[F]],
      desired: Username,
      iteration: Int = 0): Username = {
    val username = Username(
      desired.value + (if (iteration > 0) s"-$iteration" else ""))
    clients.find { case (_, client) => client.username === Some(username) } match {
      case None    => username
      case Some(_) => determineUniqueUsername(clients, desired, iteration + 1)
    }
  }
}
