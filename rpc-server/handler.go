package main

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

//func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
//	resp := rpc.NewSendResponse()
//	resp.Code, resp.Msg = areYouLucky()
//	return resp, nil
//}
//
//func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
//	resp := rpc.NewPullResponse()
//	resp.Code, resp.Msg = areYouLucky()
//	return resp, nil
//}
//
//func areYouLucky() (int32, string) {
//	if rand.Int31n(2) == 1 {
//		return 0, "success"
//	} else {
//		return 500, "oops"
//	}
//}
