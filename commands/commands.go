package commands

type Command struct {
	ID     string   `json:"id"`
	Gid    int      `json:"gid"`
	Nid    int      `json:"nid"`
	Cmd    string   `json:"cmd"`
	Roles  []string `json:"roles"`
	Fanout bool     `json:"fanout"`
	Data   string   `json:"data"`
	Args   struct {
		Name string `json:"name"`
	} `json:"args"`
}

type Result struct {
	ID        string `json:"id"`
	Nid       int    `json:"nid"`
	Gid       int    `json:"gid"`
	State     string `json:"state"`
	Data      string `json:"data"`
	Level     int    `json:"level"`
	StartTime int64  `json:"starttime"`
}
