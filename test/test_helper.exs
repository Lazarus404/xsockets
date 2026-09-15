ExUnit.start()

for start <- [
      {XSockets.SockSupervisor, :start_link, [[]]},
      {XSockets.TierSupervisor.Task, :start_link, [[]]},
      {XSockets.TierSupervisor.Pool, :start_link, [[]]}
    ] do
  case apply(elem(start, 0), elem(start, 1), elem(start, 2)) do
    {:ok, _} -> :ok
    {:error, {:already_started, _}} -> :ok
  end
end
