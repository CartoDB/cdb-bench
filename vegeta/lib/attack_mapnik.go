package vegeta

import (
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"
  "encoding/json"
	//"net"
	//"net/http"
)

type WindshaftResponse struct {
  Render int
}
// Attack reads its Targets from the passed Targeter and attacks them at
// the rate specified for duration time. Results are put into the returned channel
// as soon as they arrive.
func (a *Attacker) WindshaftAttack(tr Targeter, rate uint64, du time.Duration) <-chan *Result {
	workers := &sync.WaitGroup{}
	results := make(chan *Result)
	ticks := make(chan time.Time)
	for i := uint64(0); i < a.workers; i++ {
		go a.windshaftAttack(tr, workers, ticks, results)
	}

	go func() {
		defer close(results)
		defer workers.Wait()
		defer close(ticks)
		interval := 1e9 / rate
		hits := rate * uint64(du.Seconds())
		for began, done := time.Now(), uint64(0); done < hits; done++ {
			now, next := time.Now(), began.Add(time.Duration(done*interval))
			time.Sleep(next.Sub(now))
			select {
			case ticks <- max(next, now):
			case <-a.stopch:
				return
			default: // all workers are blocked. start one more and try again
				go a.windshaftAttack(tr, workers, ticks, results)
				done--
			}
		}
	}()

	return results
}

// Stop stops the current attack.

func (a *Attacker) windshaftAttack(tr Targeter, workers *sync.WaitGroup, ticks <-chan time.Time, results chan<- *Result) {
	workers.Add(1)
	defer workers.Done()
	for tm := range ticks {
		results <- a.windshaftHit(tr, tm)
	}
}

func (a *Attacker) windshaftHit(tr Targeter, tm time.Time) *Result {
	var (
		res = Result{Timestamp: tm}
		err error
	)

	defer func() {
		//res.Latency = time.Since(tm)
		if err != nil {
			res.Error = err.Error()
		}
	}()

	tgt, err := tr()
	if err != nil {
		return &res
	}

	req, err := tgt.Request()
	if err != nil {
		return &res
	}

	r, err := a.client.Do(req)
	if err != nil {
		// ignore redirect errors when the user set --redirects=NoFollow
		if a.redirects == NoFollow && strings.Contains(err.Error(), "stopped after") {
			err = nil
		}
		return &res
	}
	defer r.Body.Close()

	in, err := io.Copy(ioutil.Discard, r.Body)
	if err != nil {
		return &res
	}
	res.BytesIn = uint64(in)

	if req.ContentLength != -1 {
		res.BytesOut = uint64(req.ContentLength)
	}

	if res.Code = uint16(r.StatusCode); res.Code < 200 || res.Code >= 400 {
		res.Error = r.Status
	}
  var profile WindshaftResponse
  json.Unmarshal([]byte(r.Header.Get("X-Tiler-Profiler")), &profile)
  res.Latency = time.Duration(profile.Render) * time.Millisecond

	return &res
}

