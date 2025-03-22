open Ocaml_twixt_exchange
open Ocaml_twixt_lib

let send_error ?(status = `Bad_Request) content =
  Types.{ error = content }
  |> Types.yojson_of_error_doc
  |> Yojson.Safe.to_string
  |> Dream.json ~status
;;

let json_receiver json_parser handler request =
  let%lwt body = Dream.body request in
  let parse =
    try Some (body |> Yojson.Safe.from_string |> json_parser) with
    | _ -> None
  in
  match parse with
  | Some doc -> handler doc request
  | None -> send_error "Received invalid JSON input"
;;

let refresh (pb : Types.refresh_request_doc) _ =
  match Token.SessionToken.parse_token pb.refresh_token Serverconfig.token_secret with
  | Token.SessionToken.Expired -> send_error "expired token"
  | Token.SessionToken.Malformed -> send_error "malformed token"
  | Token.SessionToken.Ok token ->
    if token.token_type <> Token.SessionToken.Refresh
    then send_error "wrong token type: expected refresh token"
    else (
      match%lwt Store.Session.fetch ~sess_id:token.sess_id with
      | Ok sess ->
        if sess.current_rotation <> token.rot
        then (
          let%lwt _ = Store.Session.revoke ~sess_id:token.sess_id in
          send_error "refresh token already used")
        else (
          let%lwt _ = Store.Session.rotate ~sess_id:token.sess_id in
          let access, refresh =
            Token.SessionToken.create_token_pair
              ~access_ttl:1
              ~refresh_ttl:10
              ~sess_id:token.sess_id
              ~user_id:token.user_id
              ~secret:Serverconfig.token_secret
              ~rotation:(token.rot + 1)
          in
          Types.{ access_token = access; refresh_token = refresh }
          |> Types.yojson_of_login_response_doc
          |> Yojson.Safe.to_string
          |> Dream.json ~status:`OK)
      | Error _ -> send_error ~status:`Internal_Server_Error "internal server error")
;;

let login (pb : Types.login_request_doc) _ =
  match%lwt Store.User.login ~email:pb.email ~password:pb.password with
  | Ok (id, true) ->
    let sess_id = Id.generate () in
    let access, refresh =
      Token.SessionToken.create_token_pair
        ~access_ttl:1
        ~refresh_ttl:10
        ~sess_id
        ~user_id:id
        ~secret:Serverconfig.token_secret
        ~rotation:0
    in
    (match%lwt Store.Session.create ~user_id:id ~sess_id with
     | Ok _ ->
       Types.{ access_token = access; refresh_token = refresh }
       |> Types.yojson_of_login_response_doc
       |> Yojson.Safe.to_string
       |> Dream.json ~status:`OK
     | Error _ -> send_error ~status:`Internal_Server_Error "internal server error")
  | Ok (_, false) | Error _ -> send_error ~status:`Forbidden "bad credentials"
;;

let register _ = Dream.html "Good morning, world!"
let get_leaderboard _ = Dream.html "Good morning, world!"
let get_user_by_id _ = Dream.html "Good morning, world!"
let delete_self _ = Dream.html "Good morning, world!"
let get_game_list _ = Dream.html "Good morning, world!"

module ConnManager = struct
  type sesstrack_t =
    { mutable sess_id : Id.t
    ; mutable user_id : Id.t
    ; mutable expiry : int
    }

  module IdSet = Set.Make (Id)

  type subs_t = (Id.t, IdSet.t) Hashtbl.t

  type t =
    { subs : subs_t
    ; tracking : (Id.t, Dream.websocket * Id.t option * sesstrack_t option) Hashtbl.t
    }

  let create () = { subs = Hashtbl.create 5; tracking = Hashtbl.create 5 }

  let get_track cmgr cid =
    let _, sid, d = Hashtbl.find cmgr.tracking cid in
    sid, d
  ;;

  let track cmgr conn =
    let id = Id.generate () in
    Hashtbl.replace cmgr.tracking id (conn, None, None);
    id
  ;;

  let _subscribe subs gid cid =
    match Hashtbl.find_opt subs gid with
    | None -> Hashtbl.replace subs gid (IdSet.singleton cid)
    | Some set -> Hashtbl.replace subs gid (IdSet.add cid set)
  ;;

  let _unsubscribe subs gid cid =
    match Hashtbl.find_opt subs gid with
    | None -> ()
    | Some set -> Hashtbl.replace subs gid (IdSet.remove cid set)
  ;;

  let subscribe cmgr cid gid =
    match Hashtbl.find cmgr.tracking cid with
    | c, Some previousgid, t ->
      _unsubscribe cmgr.subs previousgid cid;
      _subscribe cmgr.subs gid cid;
      Hashtbl.replace cmgr.tracking cid (c, Some gid, t)
    | c, None, t ->
      _subscribe cmgr.subs gid cid;
      Hashtbl.replace cmgr.tracking cid (c, Some gid, t)
  ;;

  let unsubscribe cmgr cid =
    match Hashtbl.find cmgr.tracking cid with
    | c, Some previousgid, t ->
      _unsubscribe cmgr.subs previousgid cid;
      Hashtbl.replace cmgr.tracking cid (c, None, t)
    | _ -> ()
  ;;

  let set_session cmgr cid trackd =
    match Hashtbl.find cmgr.tracking cid with
    | _, _, Some td ->
      td.expiry <- trackd.expiry;
      td.sess_id <- trackd.sess_id;
      td.user_id <- trackd.user_id
    | c, s, None -> Hashtbl.replace cmgr.tracking cid (c, s, Some trackd)
  ;;

  let forget cmgr cid =
    unsubscribe cmgr cid;
    Hashtbl.remove cmgr.tracking cid
  ;;

  let iter cmgr gid f =
    let s = Hashtbl.find cmgr.subs gid in
    IdSet.fold
      (fun x acc ->
        match Hashtbl.find_opt cmgr.tracking x with
        | None -> acc
        | Some (conn, _, _) -> f conn :: acc)
      s
      []
  ;;
end

let websocket client (cid, cmgr) (message : Wsmes.complete_message) =
  match message.data with
  | Wsmes.QBoard ->
    (match ConnManager.get_track cmgr cid with
     | None, _ ->
       Wsmes.{ data = Wsmes.RNotOk "not subscribed to any board"; id = message.id }
       |> Wsmes.json_of
       |> Dream.send client
     | Some gid, _ ->
       (match%lwt Store.Game.Cache.fetch_opt gid with
        | Ok (Some cg) ->
          Wsmes.
            { data = Wsmes.RBoard (cg.board.size, Journal.to_string cg.journal)
            ; id = message.id
            }
          |> Wsmes.json_of
          |> Dream.send client
        | Ok None ->
          Wsmes.{ data = Wsmes.RNotOk "unknown game"; id = message.id }
          |> Wsmes.json_of
          |> Dream.send client
        | Error _ ->
          Wsmes.{ data = Wsmes.RNotOk "internal server error"; id = message.id }
          |> Wsmes.json_of
          |> Dream.send client))
  | Wsmes.QChatHistory _ -> Dream.send client "hi"
  | Wsmes.MChatSend _ -> Dream.send client "hi"
  | Wsmes.MGameAction _ -> Dream.send client "hi"
  | Wsmes.MResign -> Dream.send client "hi"
  | Wsmes.MDraw ->
    (match ConnManager.get_track cmgr cid with
     | None, _ ->
       Wsmes.{ data = Wsmes.RNotOk "not subscribed to any board"; id = message.id }
       |> Wsmes.json_of
       |> Dream.send client
     | _, None ->
       Wsmes.{ data = Wsmes.RNotOk "not logged in"; id = message.id }
       |> Wsmes.json_of
       |> Dream.send client
     | Some gid, Some { user_id; _ } ->
       (match%lwt Store.Game.Cache.fetch_opt gid with
        | Ok (Some cg) ->
          if cg.game_info.red = user_id || cg.game_info.black = user_id
          then (
            let res = Wsmes.{ data = Wsmes.DrawAsked; id = None } |> Wsmes.json_of in
            ConnManager.iter cmgr gid (fun c -> Dream.send c res) |> Lwt.pick)
          else
            Wsmes.{ data = Wsmes.RNotOk "not playing"; id = message.id }
            |> Wsmes.json_of
            |> Dream.send client
        | Ok None ->
          Wsmes.{ data = Wsmes.RNotOk "unknown game"; id = message.id }
          |> Wsmes.json_of
          |> Dream.send client
        | Error _ ->
          Wsmes.{ data = Wsmes.RNotOk "internal server error"; id = message.id }
          |> Wsmes.json_of
          |> Dream.send client))
  | Wsmes.Subscribe sid ->
    (match%lwt Store.Game.fetch ~id:sid with
     | Ok _ ->
       ConnManager.subscribe cmgr cid sid;
       Wsmes.{ data = Wsmes.ROk ""; id = message.id }
       |> Wsmes.json_of
       |> Dream.send client
     | Error _ ->
       Wsmes.{ data = Wsmes.RNotOk "error"; id = message.id }
       |> Wsmes.json_of
       |> Dream.send client)
  | Wsmes.Unsubscribe ->
    ConnManager.unsubscribe cmgr cid;
    Wsmes.{ data = Wsmes.ROk ""; id = message.id } |> Wsmes.json_of |> Dream.send client
  | Wsmes.Auth token ->
    (match Token.SessionToken.parse_token token Serverconfig.token_secret with
     | Token.SessionToken.Malformed ->
       Wsmes.{ data = Wsmes.RNotOk "malformed token"; id = message.id }
       |> Wsmes.json_of
       |> Dream.send client
     | Token.SessionToken.Expired ->
       Wsmes.{ data = Wsmes.RNotOk "expired token"; id = message.id }
       |> Wsmes.json_of
       |> Dream.send client
     | Ok payload ->
       if payload.token_type <> Token.SessionToken.Access
       then
         Wsmes.{ data = Wsmes.RNotOk "wrong token type"; id = message.id }
         |> Wsmes.json_of
         |> Dream.send client
       else (
         ConnManager.set_session
           cmgr
           cid
           ConnManager.
             { user_id = payload.user_id
             ; sess_id = payload.sess_id
             ; expiry = payload.expires_at
             };
         Wsmes.{ data = Wsmes.ROk ""; id = message.id }
         |> Wsmes.json_of
         |> Dream.send client))
  | _ ->
    Wsmes.{ data = Wsmes.RNotOk "unexpected input"; id = message.id }
    |> Wsmes.json_of
    |> Dream.send client
;;
