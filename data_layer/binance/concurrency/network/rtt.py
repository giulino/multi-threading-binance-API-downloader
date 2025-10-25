
def RoundTimeTripEMA(alpha: float = 0.7):
        """
        Closure that keeps an EMA of RTT (in SECONDS).
        Call it with a new RTT (seconds); it returns the updated EMA.
        """
        ema = None

        def update(rtt_s: float):    
            nonlocal ema  # use outer scope ema
            if ema is None:
                  ema = rtt_s
            ema = (alpha * rtt_s) + (1 - alpha) * ema
            return ema * 1000
        return update

if __name__ == "__main__":
    print("Testing RoundTimeTripEMA closure...")

    # Initialize closure with alpha=0.7
    ema_updater = RoundTimeTripEMA(alpha=0.7)

    # Example RTT values in seconds
    sample_rtts = [0.25, 0.30, 0.28, 0.32, 0.27, 0.35]

    print("RTT values: ", sample_rtts)
    for i, rtt in enumerate(sample_rtts, start=1):
        ema = ema_updater(rtt)
        print(f"Step {i}: RTT={rtt:.3f}s → EMA={ema:.3f}s")